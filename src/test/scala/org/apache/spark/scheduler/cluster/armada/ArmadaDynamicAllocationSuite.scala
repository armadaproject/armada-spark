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

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskSchedulerImpl

/** Single-threaded functional tests for backend component interactions.
  *
  * Verifies correctness of executor lifecycle state transitions (submit, run, register, pending
  * pruning) and API contracts (idempotency, bidirectional mappings) without concurrency. For
  * multi-threaded contention and race condition tests, see [[BackendContentionSuite]].
  */
class ArmadaDynamicAllocationSuite extends AnyFunSuite with BeforeAndAfter {

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

  test("recordAndPendExecutor and getPendingExecutorCount integration") {
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
      backend.recordAndPendExecutor(s"job-$i")
    }

    assert(backend.getPendingExecutorCount === 10)
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

    override def getExecutorIds(): Seq[String] = synchronized {
      testRegisteredExecutors.toSeq
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
