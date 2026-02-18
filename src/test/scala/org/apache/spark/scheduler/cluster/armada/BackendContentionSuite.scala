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

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

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
  * For single-threaded functional correctness tests, see [[ArmadaClusterManagerBackendSuite]].
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

    // Latch ensures all threads start simultaneously. Without it one thread could finish
    // iterating the fixed 200-element dataset before the other starts, producing no contention.
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

    val active = backend.getActiveExecutorIds
    execIds.zipWithIndex.foreach {
      case (execId, i) if i % 2 == 0 =>
        active should not contain execId
      case (execId, _) =>
        active should contain(execId)
    }

    backend.getPendingExecutorCount shouldBe total / 2
  }

  // ==================================================================
  // recordExecutor idempotency across callers
  // ==================================================================

  test(
    "recordExecutor idempotency under contention" +
      " from multiple callers, (simulates watcher thread and executor RPC thread)"
  ) {
    val total    = 200
    val error    = new AtomicReference[Throwable](null)
    val results1 = new ConcurrentHashMap[String, String]()
    val results2 = new ConcurrentHashMap[String, String]()
    // Latch ensures both threads race over the same 200 job IDs simultaneously.
    val latch = new CountDownLatch(1)

    def recordAll(results: ConcurrentHashMap[String, String]): Thread = new Thread {
      override def run(): Unit =
        try {
          latch.await()
          (1 to total).foreach { i =>
            val jobId  = s"idem-job-$i"
            val execId = backend.recordExecutor(jobId)
            results.put(jobId, execId)
          }
        } catch {
          case t: Throwable => error.compareAndSet(null, t)
        }
    }

    val thread1 = recordAll(results1)
    val thread2 = recordAll(results2)
    thread1.start()
    thread2.start()
    latch.countDown()

    thread1.join(10000)
    thread2.join(10000)

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
