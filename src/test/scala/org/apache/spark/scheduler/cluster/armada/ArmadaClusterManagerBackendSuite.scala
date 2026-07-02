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

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{ExecutorLossReason, TaskSchedulerImpl}

class ArmadaClusterManagerBackendSuite extends AnyFunSuite with BeforeAndAfter with Matchers {

  var backend: ArmadaClusterManagerBackend = _
  var sc: SparkContext                     = _
  var env: SparkEnv                        = _
  var taskScheduler: TaskSchedulerImpl     = _
  var rpcEnv: RpcEnv                       = _
  var sparkConf: SparkConf                 = _

  before {
    sparkConf = new SparkConf(false)
      .set("spark.app.id", "test-app-123")
      // Short grace period so backend.stop() in `after` doesn't block the suite for 30s each run.
      .set("spark.armada.killGracePeriod", "10ms")

    sc = mock(classOf[SparkContext])
    env = mock(classOf[SparkEnv])
    taskScheduler = mock(classOf[TaskSchedulerImpl])
    rpcEnv = mock(classOf[RpcEnv])

    // Mock ResourceProfileManager
    val resourceProfileManager = mock(classOf[org.apache.spark.resource.ResourceProfileManager])
    val defaultResourceProfile = mock(classOf[org.apache.spark.resource.ResourceProfile])

    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(env)
    when(sc.resourceProfileManager).thenReturn(resourceProfileManager)
    when(resourceProfileManager.defaultResourceProfile).thenReturn(defaultResourceProfile)
    when(taskScheduler.sc).thenReturn(sc)
    when(env.rpcEnv).thenReturn(rpcEnv)

    val executorService = Executors.newScheduledThreadPool(1)
    backend = new ArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      executorService,
      "armada://localhost:50051"
    )
  }

  after {
    if (backend != null) {
      try { backend.stop() }
      catch { case NonFatal(_) => }
    }
  }

  private def ignoreRpcErrors(block: => Unit): Unit =
    ArmadaBackendTestUtils.ignoreRpcErrors(block)

  test("recordExecutor returns same ID for duplicate job") {
    val id1 = backend.recordExecutor("job-123")
    val id2 = backend.recordExecutor("job-123")

    id1 shouldBe id2
  }

  test("recordExecutor assigns unique IDs for different jobs") {
    val id1 = backend.recordExecutor("job-123")
    val id2 = backend.recordExecutor("job-456")

    id1 should not be id2
  }

  test("recordExecutor increments ID counter") {
    val id1 = backend.recordExecutor("job-1").toInt
    val id2 = backend.recordExecutor("job-2").toInt
    val id3 = backend.recordExecutor("job-3").toInt

    id2 shouldBe id1 + 1
    id3 shouldBe id2 + 1
  }

  test("recordAndPendExecutor and getPendingExecutorCount work correctly") {
    backend.getPendingExecutorCount shouldBe 0

    backend.recordAndPendExecutor("job-1")
    backend.getPendingExecutorCount shouldBe 1

    backend.recordAndPendExecutor("job-2")
    backend.recordAndPendExecutor("job-3")
    backend.getPendingExecutorCount shouldBe 3
  }

  test("recordAndPendExecutor is thread-safe") {
    val numThreads         = 10
    val executorsPerThread = 100

    val threads = (0 until numThreads).map { i =>
      new Thread {
        override def run(): Unit = {
          (0 until executorsPerThread).foreach { j =>
            backend.recordAndPendExecutor(s"job-$i-$j")
          }
        }
      }
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    backend.getPendingExecutorCount shouldBe numThreads * executorsPerThread
  }

  test("onExecutorSubmitted adds to pending") {
    backend.getPendingExecutorCount shouldBe 0
    backend.onExecutorSubmitted("job-789")
    backend.getPendingExecutorCount shouldBe 1
  }

  test("applicationId returns correct app ID") {
    backend.applicationId() shouldBe "test-app-123"
  }

  test("recordExecutor is idempotent with concurrent calls") {
    val jobId      = "concurrent-job"
    val numThreads = 20
    val results    = new java.util.concurrent.ConcurrentHashMap[String, String]()

    val threads = (0 until numThreads).map { i =>
      new Thread {
        override def run(): Unit = {
          val id = backend.recordExecutor(jobId)
          results.put(s"thread-$i", id)
        }
      }
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    val uniqueIds = results.values().toArray.toSet
    uniqueIds.size shouldBe 1
  }

  test("getActiveExecutorIds excludes failed executors") {
    val execId1 = backend.recordExecutor("job-1")
    val execId2 = backend.recordExecutor("job-2")

    ignoreRpcErrors { backend.onExecutorFailed("job-1", execId1, 1, "OOM") }

    val active = backend.getActiveExecutorIds
    active should not contain execId1
    active should contain(execId2)
  }

  test("getActiveExecutorIds excludes succeeded executors") {
    val execId1 = backend.recordExecutor("job-1")
    val execId2 = backend.recordExecutor("job-2")

    ignoreRpcErrors { backend.onExecutorSucceeded("job-1", execId1) }

    val active = backend.getActiveExecutorIds
    active should not contain execId1
    active should contain(execId2)
  }

  test("getActiveExecutorIds excludes cancelled executors") {
    val execId1 = backend.recordExecutor("job-1")
    val execId2 = backend.recordExecutor("job-2")

    ignoreRpcErrors { backend.onExecutorCancelled("job-1", execId1) }

    val active = backend.getActiveExecutorIds
    active should not contain execId1
    active should contain(execId2)
  }

  test("getActiveExecutorIds excludes unschedulable executors") {
    val execId1 = backend.recordExecutor("job-1")
    val execId2 = backend.recordExecutor("job-2")

    ignoreRpcErrors {
      backend.onExecutorUnableToSchedule("job-1", execId1, "No resources")
    }

    val active = backend.getActiveExecutorIds
    active should not contain execId1
    active should contain(execId2)
  }

  test("terminal executors are removed from pendingExecutors") {
    backend.onExecutorSubmitted("job-1")
    backend.onExecutorSubmitted("job-2")
    backend.getPendingExecutorCount shouldBe 2

    val execId1 = backend.recordExecutor("job-1")
    ignoreRpcErrors { backend.onExecutorFailed("job-1", execId1, 1, "failed") }

    backend.getPendingExecutorCount shouldBe 1
  }

  test("pending executor lifecycle - submit to running") {
    val executorService = Executors.newScheduledThreadPool(1)
    val testableBackend = new TestableArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      executorService,
      "armada://localhost:50051"
    )

    try {
      val jobId = "job-lifecycle-test"

      testableBackend.getPendingExecutorCount shouldBe 0

      // Submit executor - should be added to pending
      testableBackend.onExecutorSubmitted(jobId)
      testableBackend.getPendingExecutorCount shouldBe 1

      val execId = testableBackend.recordExecutor(jobId)

      // Mark as running - executor stays in pending until it registers with Spark
      testableBackend.onExecutorRunning(jobId, execId)
      testableBackend.getPendingExecutorCount shouldBe 1

      testableBackend.simulateExecutorRegistration(execId)

      testableBackend.getPendingExecutorCount shouldBe 0
    } finally {
      try { testableBackend.stop() }
      catch { case NonFatal(_) => }
      executorService.shutdownNow()
    }
  }

  // Use multiple threads to terminate half the jobs, then confirm the number
  // of remaining active ones.
  test("thread safety of terminal executor tracking") {
    val numJobs = 100
    val jobIds  = (1 to numJobs).map(i => s"job-$i")
    val execIds = jobIds.map(backend.recordExecutor)

    val threads = execIds.zipWithIndex.collect {
      case (execId, i) if i % 2 == 0 =>
        new Thread {
          override def run(): Unit = {
            ignoreRpcErrors {
              backend.onExecutorFailed(jobIds(i), execId, 1, "failed")
            }
          }
        }
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    backend.getActiveExecutorIds.size shouldBe numJobs / 2
  }

  test("isStopping is false before stop() and true after") {
    sparkConf.set("spark.armada.deleteExecutors", "false")
    backend.isStopping shouldBe false
    ignoreRpcErrors { backend.stop() }
    backend.isStopping shouldBe true
  }

  // ========================================================================
  // Pre-marked-removal tests
  //
  // CoarseGrainedSchedulerBackend.removeExecutor reads executorsPendingToRemove via
  //   executorsPendingToRemove.remove(executorId).getOrElse(false)
  // The boolean value MUST be true to flip the rewrite-to-ExecutorKilled branch on
  // (and to skip task-failure accounting). Storing false silently disables the rewrite.
  // ========================================================================

  test("onExecutorSucceeded pre-marks executor with value true") {
    val execId = backend.recordExecutor("job-1")
    ignoreRpcErrors { backend.onExecutorSucceeded("job-1", execId) }
    backend.executorsPendingToRemove.get(execId) shouldBe Some(true)
  }

  test("doKillExecutors pre-marks executor with value true") {
    val execId = backend.recordExecutor("job-1")
    ignoreRpcErrors { backend.doKillExecutors(Seq(execId)) }
    backend.executorsPendingToRemove.get(execId) shouldBe Some(true)
  }

  test("stop pre-marks all known executors with value true") {
    sparkConf.set("spark.armada.deleteExecutors", "false")
    val executorService = Executors.newScheduledThreadPool(1)
    val testable = new TestableArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      executorService,
      "armada://localhost:50051"
    )
    try {
      testable.simulateExecutorRegistration("e1")
      testable.simulateExecutorRegistration("e2")
      ignoreRpcErrors { testable.stop() }
      testable.executorsPendingToRemove.get("e1") shouldBe Some(true)
      testable.executorsPendingToRemove.get("e2") shouldBe Some(true)
    } finally {
      executorService.shutdownNow()
    }
  }

  // ========================================================================
  // Dedup tests
  //
  // onDisconnected and the Armada event watcher race to report the same exit. Without
  // the removedExecutors gate, both call safeRemoveExecutor -> removeExecutor, and Spark's
  // TaskSchedulerImpl logs "Lost an executor X (already removed)" on the second call.
  // ========================================================================

  test("safeRemoveExecutor only calls removeExecutor once per executor") {
    val executorService = Executors.newScheduledThreadPool(1)
    val testable = new TestableArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      executorService,
      "armada://localhost:50051"
    )
    try {
      val execId = testable.recordExecutor("job-1")
      // Two event-watcher callbacks for the same executor (succeed-then-fail can happen if
      // events arrive after we've also seen a disconnect).
      testable.onExecutorSucceeded("job-1", execId)
      testable.onExecutorFailed("job-1", execId, 1, "after-the-fact failure")
      // Second call must be a no-op: removeExecutor on the parent must have run exactly once.
      testable.removeExecutorCallCount.get() shouldBe 1
    } finally {
      executorService.shutdownNow()
    }
  }

  private class TestableArmadaClusterManagerBackend(
      scheduler: TaskSchedulerImpl,
      sc: SparkContext,
      executorService: java.util.concurrent.ScheduledExecutorService,
      masterURL: String
  ) extends ArmadaClusterManagerBackend(scheduler, sc, executorService, masterURL) {

    private val testRegisteredExecutors = scala.collection.mutable.Set.empty[String]

    val removeExecutorCallCount = new AtomicInteger(0)

    def simulateExecutorRegistration(executorId: String): Unit = {
      testRegisteredExecutors += executorId
    }

    // in real life, super.getExecutorIds() would be used here, but that
    // isn't being used in this test
    override def getExecutorIds(): Seq[String] = synchronized {
      testRegisteredExecutors.toSeq
    }

    // Count removeExecutor invocations and skip super so unit tests don't trigger
    // NPE from the parent's `driverEndpoint.send(...)`.
    override protected def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      removeExecutorCallCount.incrementAndGet()
    }
  }
}
