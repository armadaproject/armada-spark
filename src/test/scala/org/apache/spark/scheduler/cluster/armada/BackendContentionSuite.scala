/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.armada

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, Executors}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskSchedulerImpl

/** Multi-threaded contention tests for ArmadaClusterManagerBackend.
  *
  * Exercises all four production thread roles concurrently to verify lock ordering, atomicity, and
  * absence of deadlocks:
  *   - Event watcher: ArmadaEventWatcher daemon processing gRPC job events
  *   - Allocator: ArmadaExecutorAllocator on ScheduledExecutorService polling demand vs supply
  *   - Spark scheduler: Spark core calling doKillExecutors to scale down
  *   - RPC: ArmadaDriverEndpoint handling GenerateExecID messages from executors
  *
  * For single-threaded functional correctness tests, see [[ArmadaDynamicAllocationSuite]].
  */
class BackendContentionSuite extends AnyFunSuite with BeforeAndAfter with Matchers {

  var backend: ArmadaClusterManagerBackend = _
  var sc: SparkContext                     = _
  var sparkConf: SparkConf                 = _

  before {
    sparkConf = new SparkConf(false)
      .set("spark.app.id", "contention-test-app")

    sc = createMockSparkContext(sparkConf)
    val taskScheduler = createMockTaskScheduler(sc)

    backend = new ArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      Executors.newScheduledThreadPool(1),
      "armada://localhost:50051"
    )
  }

  after {
    if (backend != null) {
      try { backend.stop() }
      catch { case NonFatal(_) => }
    }
  }

  /** Ignores NullPointerException from removeExecutor when driverEndpoint is null in tests.
    */
  private def ignoreRpcErrors(block: => Unit): Unit = {
    try { block }
    catch { case _: NullPointerException => }
  }

  // ==================================================================
  // Terminal executor cannot be re-added to pending
  // ==================================================================

  test(
    "concurrent markTerminal + recordAndPendExecutor:" +
      " no executor stuck in both sets"
  ) {
    val total = 200
    val error = new AtomicReference[Throwable](null)

    // Pre-register all executors
    val execIds = (1 to total).map { i =>
      backend.recordAndPendExecutor(s"job-$i")
    }

    val latch = new CountDownLatch(1)

    // Terminator: marks even-indexed executors terminal
    val terminator = new Thread {
      override def run(): Unit =
        try {
          latch.await()
          execIds.zipWithIndex.foreach {
            case (execId, i) if i % 2 == 0 =>
              ignoreRpcErrors {
                backend.onExecutorFailed(
                  s"job-${i + 1}",
                  execId,
                  1,
                  "test"
                )
              }
            case _ =>
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // Re-submitter: calls recordAndPendExecutor for all jobs
    val reSubmitter = new Thread {
      override def run(): Unit =
        try {
          latch.await()
          (1 to total).foreach { i =>
            backend.recordAndPendExecutor(s"job-$i")
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // Reader: polls counts
    val readerDone = new AtomicBoolean(false)
    val reader = new Thread {
      override def run(): Unit =
        try {
          latch.await()
          while (!readerDone.get() && error.get() == null) {
            val pending = backend.getPendingExecutorCount
            pending should be >= 0
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    terminator.start()
    reSubmitter.start()
    reader.start()
    latch.countDown()

    terminator.join(10000)
    reSubmitter.join(10000)
    readerDone.set(true)
    reader.join(10000)

    error.get() shouldBe null

    val active = backend.getActiveExecutorIds()
    execIds.zipWithIndex.foreach {
      case (execId, i) if i % 2 == 0 =>
        active should not contain execId
      case (execId, _) =>
        active should contain(execId)
    }

    backend.getPendingExecutorCount should be >= 0
  }

  // ==================================================================
  // getExecutorSnapshot consistency
  // ==================================================================

  test(
    "getExecutorSnapshot returns consistent pair under contention"
  ) {
    // Replace default backend with testable variant
    try { backend.stop() }
    catch { case NonFatal(_) => }

    val testableBackend = createTestableBackend()
    backend = testableBackend

    val error          = new AtomicReference[Throwable](null)
    val done           = new AtomicBoolean(false)
    val totalSubmitted = new AtomicInteger(0)

    val submitter = new Thread {
      override def run(): Unit =
        try {
          var i = 0
          while (!done.get() && error.get() == null) {
            i += 1
            testableBackend.recordAndPendExecutor(s"snap-job-$i")
            totalSubmitted.incrementAndGet()
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    val registrar = new Thread {
      override def run(): Unit =
        try {
          var lastRegistered = 0
          while (!done.get() && error.get() == null) {
            val current = totalSubmitted.get()
            ((lastRegistered + 1) to current).foreach { i =>
              val execId =
                testableBackend.recordExecutor(s"snap-job-$i")
              testableBackend.simulateExecutorRegistration(execId)
            }
            lastRegistered = current
            Thread.sleep(1)
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    val reader = new Thread {
      override def run(): Unit =
        try {
          while (!done.get() && error.get() == null) {
            val (registered, pending) =
              testableBackend.getExecutorSnapshot
            registered should be >= 0
            pending should be >= 0
            (registered + pending) should be <= totalSubmitted.get()
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    submitter.start()
    registrar.start()
    reader.start()

    Thread.sleep(3000)
    done.set(true)

    submitter.join(5000)
    registrar.join(5000)
    reader.join(5000)

    error.get() shouldBe null
  }

  // ==================================================================
  // four-role soak test
  // ==================================================================

  test(
    "four-role soak test simulates production thread boundaries"
  ) {
    val error         = new AtomicReference[Throwable](null)
    val done          = new AtomicBoolean(false)
    val submittedJobs = new ConcurrentLinkedQueue[String]()
    val jobCounter    = new AtomicInteger(0)

    // Event watcher: submit then randomly terminate
    val eventWatcher = new Thread {
      override def run(): Unit =
        try {
          val rng = new java.util.Random(42)
          while (!done.get() && error.get() == null) {
            val jobId =
              s"soak-job-${jobCounter.incrementAndGet()}"
            backend.onExecutorSubmitted(jobId)
            submittedJobs.add(jobId)

            if (rng.nextInt(3) == 0) {
              val execId = backend.recordExecutor(jobId)
              ignoreRpcErrors {
                rng.nextInt(3) match {
                  case 0 =>
                    backend.onExecutorFailed(
                      jobId,
                      execId,
                      1,
                      "test"
                    )
                  case 1 =>
                    backend.onExecutorSucceeded(jobId, execId)
                  case _ =>
                    backend.onExecutorCancelled(jobId, execId)
                }
              }
            }
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // Allocator: snapshot then submit if gap
    val allocator = new Thread {
      override def run(): Unit =
        try {
          while (!done.get() && error.get() == null) {
            val (registered, pending) =
              backend.getExecutorSnapshot
            registered should be >= 0
            pending should be >= 0

            if (pending < 5) {
              val jobId =
                s"alloc-job-${jobCounter.incrementAndGet()}"
              backend.recordAndPendExecutor(jobId)
              submittedJobs.add(jobId)
            }
            Thread.sleep(1)
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // Spark scheduler: kill random active executors
    val scheduler = new Thread {
      override def run(): Unit =
        try {
          val rng = new java.util.Random(99)
          while (!done.get() && error.get() == null) {
            val active = backend.getActiveExecutorIds()
            if (active.nonEmpty) {
              val victim = active(rng.nextInt(active.size))
              ignoreRpcErrors {
                backend.onExecutorCancelled(
                  s"kill-$victim",
                  victim
                )
              }
            }
            Thread.sleep(1)
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // RPC: recordExecutor for already-submitted jobs
    val rpc = new Thread {
      override def run(): Unit =
        try {
          while (!done.get() && error.get() == null) {
            val jobId = submittedJobs.poll()
            if (jobId != null) {
              backend.recordExecutor(jobId)
            }
            Thread.sleep(1)
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    eventWatcher.start()
    allocator.start()
    scheduler.start()
    rpc.start()

    Thread.sleep(3000)
    done.set(true)

    eventWatcher.join(5000)
    allocator.join(5000)
    scheduler.join(5000)
    rpc.join(5000)

    error.get() shouldBe null
    backend.getPendingExecutorCount should be >= 0
    backend.getActiveExecutorIds().size should
      be <= jobCounter.get()
  }

  // ==================================================================
  // rapid submit-then-terminate
  // ==================================================================

  test(
    "rapid submit-then-terminate does not leak pending executors"
  ) {
    val total = 500
    val error = new AtomicReference[Throwable](null)
    val queue = new ConcurrentLinkedQueue[(String, String)]()

    // Submitter pushes all jobs first
    val submitter = new Thread {
      override def run(): Unit =
        try {
          (1 to total).foreach { i =>
            val jobId  = s"leak-job-$i"
            val execId = backend.recordAndPendExecutor(jobId)
            queue.add((jobId, execId))
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    submitter.start()
    submitter.join(10000)

    // Terminator drains and terminates
    val terminator = new Thread {
      override def run(): Unit =
        try {
          var pair = queue.poll()
          while (pair != null) {
            ignoreRpcErrors {
              backend.onExecutorFailed(
                pair._1,
                pair._2,
                1,
                "immediate fail"
              )
            }
            pair = queue.poll()
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    terminator.start()
    terminator.join(10000)

    error.get() shouldBe null
    backend.getPendingExecutorCount shouldBe 0
    backend.getActiveExecutorIds() shouldBe empty
  }

  // ==================================================================
  // deadlock detection
  // ==================================================================

  test(
    "getPendingExecutorCount does not deadlock under contention"
  ) {
    val error      = new AtomicReference[Throwable](null)
    val done       = new AtomicBoolean(false)
    val iterations = Array.fill(4)(new AtomicInteger(0))
    val jobCounter = new AtomicInteger(0)

    // Allocator: recordAndPendExecutor then getExecutorSnapshot
    val allocatorThread = new Thread {
      override def run(): Unit =
        try {
          while (!done.get() && error.get() == null) {
            val jobId =
              s"dl-alloc-${jobCounter.incrementAndGet()}"
            backend.recordAndPendExecutor(jobId)
            backend.getExecutorSnapshot
            iterations(0).incrementAndGet()
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // Event watcher: onExecutorSubmitted then onExecutorFailed
    val watcherThread = new Thread {
      override def run(): Unit =
        try {
          while (!done.get() && error.get() == null) {
            val jobId =
              s"dl-watch-${jobCounter.incrementAndGet()}"
            backend.onExecutorSubmitted(jobId)
            val execId = backend.recordExecutor(jobId)
            ignoreRpcErrors {
              backend.onExecutorFailed(
                jobId,
                execId,
                1,
                "test"
              )
            }
            iterations(1).incrementAndGet()
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // Scheduler: getActiveExecutorIds
    val schedulerThread = new Thread {
      override def run(): Unit =
        try {
          while (!done.get() && error.get() == null) {
            backend.getActiveExecutorIds()
            iterations(2).incrementAndGet()
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // RPC: recordExecutor then getPendingExecutorCount
    val rpcThread = new Thread {
      override def run(): Unit =
        try {
          while (!done.get() && error.get() == null) {
            val jobId =
              s"dl-rpc-${jobCounter.incrementAndGet()}"
            backend.recordExecutor(jobId)
            backend.getPendingExecutorCount
            iterations(3).incrementAndGet()
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    allocatorThread.start()
    watcherThread.start()
    schedulerThread.start()
    rpcThread.start()

    Thread.sleep(3000)
    done.set(true)

    allocatorThread.join(5000)
    watcherThread.join(5000)
    schedulerThread.join(5000)
    rpcThread.join(5000)

    // Verify all threads finished (no deadlock)
    allocatorThread.isAlive shouldBe false
    watcherThread.isAlive shouldBe false
    schedulerThread.isAlive shouldBe false
    rpcThread.isAlive shouldBe false

    error.get() shouldBe null

    // Each thread should have completed many iterations
    iterations.foreach { counter =>
      counter.get() should be > 10
    }
  }

  // ==================================================================
  // recordExecutor idempotency across callers
  // ==================================================================

  test(
    "recordExecutor idempotency under contention" +
      " from multiple callers"
  ) {
    val total = 200
    val error = new AtomicReference[Throwable](null)
    val results1 =
      new ConcurrentHashMap[String, String]()
    val results2 =
      new ConcurrentHashMap[String, String]()
    val latch = new CountDownLatch(1)

    // RPC thread
    val rpcThread = new Thread {
      override def run(): Unit =
        try {
          latch.await()
          (1 to total).foreach { i =>
            val jobId  = s"idem-job-$i"
            val execId = backend.recordExecutor(jobId)
            results1.put(jobId, execId)
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    // Event watcher thread
    val watcherThread = new Thread {
      override def run(): Unit =
        try {
          latch.await()
          (1 to total).foreach { i =>
            val jobId  = s"idem-job-$i"
            val execId = backend.recordExecutor(jobId)
            results2.put(jobId, execId)
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    rpcThread.start()
    watcherThread.start()
    latch.countDown()

    rpcThread.join(10000)
    watcherThread.join(10000)

    error.get() shouldBe null

    // Every job maps to exactly one executor ID;
    // both threads agree on every mapping
    (1 to total).foreach { i =>
      val jobId = s"idem-job-$i"
      val id1   = results1.get(jobId)
      val id2   = results2.get(jobId)
      id1 should not be null
      id2 should not be null
      id1 shouldBe id2
    }

    // Total unique executor IDs equals total jobs
    val allIds =
      (results1.values().asScala ++ results2.values().asScala).toSet
    allIds.size shouldBe total
  }

  // ==================================================================
  // Helpers
  // ==================================================================

  private def createTestableBackend(): TestableArmadaClusterManagerBackend = {
    val taskScheduler = createMockTaskScheduler(sc)
    new TestableArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      Executors.newScheduledThreadPool(1),
      "armada://localhost:50051"
    )
  }

  private class TestableArmadaClusterManagerBackend(
      scheduler: TaskSchedulerImpl,
      sc: SparkContext,
      executorService: java.util.concurrent.ScheduledExecutorService,
      masterURL: String
  ) extends ArmadaClusterManagerBackend(
        scheduler,
        sc,
        executorService,
        masterURL
      ) {

    private val testRegisteredExecutors =
      ConcurrentHashMap.newKeySet[String]()

    def simulateExecutorRegistration(
        executorId: String
    ): Unit = {
      testRegisteredExecutors.add(executorId)
    }

    override def getExecutorIds(): Seq[String] = synchronized {
      testRegisteredExecutors.asScala.toSeq
    }
  }

  private def createMockSparkContext(
      sparkConf: SparkConf
  ): SparkContext = {
    val sc     = mock(classOf[SparkContext])
    val env    = mock(classOf[SparkEnv])
    val rpcEnv = mock(classOf[RpcEnv])

    val resourceProfileManager =
      mock(classOf[org.apache.spark.resource.ResourceProfileManager])
    val defaultResourceProfile =
      mock(classOf[org.apache.spark.resource.ResourceProfile])

    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(env)
    when(env.rpcEnv).thenReturn(rpcEnv)
    when(sc.resourceProfileManager)
      .thenReturn(resourceProfileManager)
    when(resourceProfileManager.defaultResourceProfile)
      .thenReturn(defaultResourceProfile)

    sc
  }

  private def createMockTaskScheduler(
      sc: SparkContext
  ): TaskSchedulerImpl = {
    val scheduler = mock(classOf[TaskSchedulerImpl])
    when(scheduler.sc).thenReturn(sc)
    scheduler
  }
}
