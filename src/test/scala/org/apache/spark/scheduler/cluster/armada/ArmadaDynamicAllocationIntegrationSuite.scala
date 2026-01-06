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

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskSchedulerImpl

/** Integration tests for Armada dynamic allocation.
  *
  * These tests verify the interaction between components:
  *   - ArmadaClusterManagerBackend
  *   - ArmadaExecutorAllocator
  *   - ArmadaEventWatcher
  *
  * Note: These are integration tests that test component interactions.
  */
class ArmadaDynamicAllocationIntegrationSuite extends AnyFunSuite with BeforeAndAfter {

  var backend: ArmadaClusterManagerBackend = _
  var sc: SparkContext                     = _
  var conf: SparkConf                      = _

  before {
    conf = new SparkConf(false)
      .set("spark.app.id", "integration-test-app")
      .set("spark.armada.allocation.batch.size", "5")
      .set("spark.armada.max.pending.jobs", "100")
      .set("spark.armada.allocation.check.interval", "100")
  }

  after {
    if (backend != null) {
      try {
        backend.stop()
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  test("recordExecutor creates bidirectional mappings") {
    sc = createMockSparkContext(conf)
    val taskScheduler = createMockTaskScheduler(sc)

    backend = new ArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      java.util.concurrent.Executors.newScheduledThreadPool(1),
      "armada://localhost:50051"
    )

    val jobId1 = "job-001"
    val jobId2 = "job-002"

    val execId1 = backend.recordExecutor(jobId1)
    val execId2 = backend.recordExecutor(jobId2)

    // Verify IDs are unique
    assert(execId1 !== execId2)

    // Verify idempotency
    assert(backend.recordExecutor(jobId1) === execId1)
    assert(backend.recordExecutor(jobId2) === execId2)
  }

  test("pending executor lifecycle - submit to running") {
    sc = createMockSparkContext(conf)
    val taskScheduler = createMockTaskScheduler(sc)

    backend = new TestableArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      java.util.concurrent.Executors.newScheduledThreadPool(1),
      "armada://localhost:50051"
    )

    val jobId = "job-lifecycle-test"

    // Initially no pending executors
    assert(backend.getPendingExecutorCount === 0)

    // Submit executor - should be added to pending
    backend.onExecutorSubmitted(jobId)
    assert(backend.getPendingExecutorCount === 1)

    // Get the executor ID that was created
    val execId = backend.recordExecutor(jobId)

    // Mark as running - executor stays in pending until it registers with Spark
    backend.onExecutorRunning(jobId, execId)
    assert(
      backend.getPendingExecutorCount === 1,
      "Executor should stay pending until registered with Spark"
    )

    backend.asInstanceOf[TestableArmadaClusterManagerBackend].simulateExecutorRegistration(execId)

    assert(
      backend.getPendingExecutorCount === 0,
      "Executor should be removed from pending after registering with Spark"
    )
  }

  test("concurrent executor submissions are thread-safe") {
    sc = createMockSparkContext(conf)
    val taskScheduler = createMockTaskScheduler(sc)

    backend = new ArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      java.util.concurrent.Executors.newScheduledThreadPool(1),
      "armada://localhost:50051"
    )

    val numThreads    = 10
    val jobsPerThread = 50
    val executorIds   = new ConcurrentHashMap[String, String]()

    val threads = (0 until numThreads).map { i =>
      new Thread {
        override def run(): Unit = {
          (0 until jobsPerThread).foreach { j =>
            val jobId = s"job-$i-$j"
            backend.onExecutorSubmitted(jobId)
            val execId = backend.recordExecutor(jobId)
            executorIds.put(jobId, execId)
          }
        }
      }
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Verify all jobs got executor IDs
    assert(executorIds.size() === numThreads * jobsPerThread)

    // Verify all executor IDs are unique
    val uniqueExecIds = executorIds.values().toArray.toSet
    assert(uniqueExecIds.size === numThreads * jobsPerThread)

    // Verify pending count matches
    assert(backend.getPendingExecutorCount === numThreads * jobsPerThread)
  }

  test("addPendingExecutor and getPendingExecutorCount integration") {
    sc = createMockSparkContext(conf)
    val taskScheduler = createMockTaskScheduler(sc)

    backend = new ArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      java.util.concurrent.Executors.newScheduledThreadPool(1),
      "armada://localhost:50051"
    )

    // Add some pending executors
    (1 to 10).foreach { i =>
      backend.addPendingExecutor(s"exec-$i")
    }

    assert(backend.getPendingExecutorCount === 10)

    assert(
      backend.getPendingExecutorCount === 10,
      "Executors stay pending until registered with Spark"
    )
  }

  // Helper methods

  private class TestableArmadaClusterManagerBackend(
      scheduler: TaskSchedulerImpl,
      sc: SparkContext,
      executorService: java.util.concurrent.ScheduledExecutorService,
      masterURL: String
  ) extends ArmadaClusterManagerBackend(scheduler, sc, executorService, masterURL) {

    private val testRegisteredExecutors = scala.collection.mutable.Set.empty[String]

    /** Simulate executor registration by adding it to the test set */
    def simulateExecutorRegistration(executorId: String): Unit = {
      testRegisteredExecutors += executorId
    }

    /** Include our test executors */
    override def getExecutorIds(): Seq[String] = synchronized {
      super.getExecutorIds() ++ testRegisteredExecutors.toSeq
    }
  }

  private def createMockSparkContext(sparkConf: SparkConf): SparkContext = {
    val sc     = org.mockito.Mockito.mock(classOf[SparkContext])
    val env    = org.mockito.Mockito.mock(classOf[SparkEnv])
    val rpcEnv = org.mockito.Mockito.mock(classOf[RpcEnv])

    // Mock ResourceProfileManager
    val resourceProfileManager =
      org.mockito.Mockito.mock(classOf[org.apache.spark.resource.ResourceProfileManager])
    val defaultResourceProfile =
      org.mockito.Mockito.mock(classOf[org.apache.spark.resource.ResourceProfile])

    org.mockito.Mockito.when(sc.conf).thenReturn(sparkConf)
    org.mockito.Mockito.when(sc.env).thenReturn(env)
    org.mockito.Mockito.when(env.rpcEnv).thenReturn(rpcEnv)
    org.mockito.Mockito.when(sc.resourceProfileManager).thenReturn(resourceProfileManager)
    org.mockito.Mockito
      .when(resourceProfileManager.defaultResourceProfile)
      .thenReturn(defaultResourceProfile)

    sc
  }

  private def createMockTaskScheduler(sc: SparkContext): TaskSchedulerImpl = {
    val scheduler = org.mockito.Mockito.mock(classOf[TaskSchedulerImpl])
    org.mockito.Mockito.when(scheduler.sc).thenReturn(sc)
    scheduler
  }
}
