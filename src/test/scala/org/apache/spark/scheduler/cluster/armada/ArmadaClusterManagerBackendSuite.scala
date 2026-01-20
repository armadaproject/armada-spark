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

import java.io.File
import java.util.concurrent.Executors

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.Mockito._
import org.apache.spark.deploy.armada.Config._

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskSchedulerImpl

class ArmadaClusterManagerBackendSuite extends AnyFunSuite with BeforeAndAfter {

  var backend: ArmadaClusterManagerBackend = _
  var sc: SparkContext                     = _
  var env: SparkEnv                        = _
  var taskScheduler: TaskSchedulerImpl     = _
  var rpcEnv: RpcEnv                       = _
  var sparkConf: SparkConf                 = _
  var tempDir: File                        = _

  before {
    sparkConf = new SparkConf(false)
      .set("spark.app.id", "test-app-123")

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

    tempDir = java.nio.file.Files.createTempDirectory("armada-auth-test").toFile
    tempDir.deleteOnExit()

    backend = createBackend()
  }

  after {
    if (backend != null) {
      try {
        backend.stop()
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
    // Clean up temp directory
    if (tempDir != null && tempDir.exists()) {
      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()
    }
  }

  def createBackend(): ArmadaClusterManagerBackend = {
    val executorService = Executors.newScheduledThreadPool(1)
    new ArmadaClusterManagerBackend(
      taskScheduler,
      sc,
      executorService,
      "armada://localhost:50051"
    )
  }

  test("recordExecutor returns same ID for duplicate job") {
    val jobId = "job-123"

    val id1 = backend.recordExecutor(jobId)
    val id2 = backend.recordExecutor(jobId)

    assert(id1 === id2, "recordExecutor should return same ID for same job")
  }

  test("recordExecutor assigns unique IDs for different jobs") {
    val jobId1 = "job-123"
    val jobId2 = "job-456"

    val id1 = backend.recordExecutor(jobId1)
    val id2 = backend.recordExecutor(jobId2)

    assert(id1 !== id2, "recordExecutor should return different IDs for different jobs")
  }

  test("recordExecutor increments ID counter") {
    val id1 = backend.recordExecutor("job-1").toInt
    val id2 = backend.recordExecutor("job-2").toInt
    val id3 = backend.recordExecutor("job-3").toInt

    assert(id2 === id1 + 1, "IDs should increment")
    assert(id3 === id2 + 1, "IDs should increment")
  }

  test("addPendingExecutor and getPendingExecutorCount work correctly") {
    assert(backend.getPendingExecutorCount === 0, "Should start with 0 pending")

    backend.addPendingExecutor("1")
    assert(backend.getPendingExecutorCount === 1)

    backend.addPendingExecutor("2")
    backend.addPendingExecutor("3")
    assert(backend.getPendingExecutorCount === 3)
  }

  test("addPendingExecutor is thread-safe") {
    val numThreads         = 10
    val executorsPerThread = 100

    val threads = (0 until numThreads).map { i =>
      new Thread {
        override def run(): Unit = {
          (0 until executorsPerThread).foreach { j =>
            val id = s"exec-$i-$j"
            backend.addPendingExecutor(id)
          }
        }
      }
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    assert(backend.getPendingExecutorCount === numThreads * executorsPerThread)
  }

  test("onExecutorSubmitted adds to pending") {
    val jobId = "job-789"

    assert(backend.getPendingExecutorCount === 0)

    backend.onExecutorSubmitted(jobId)

    assert(backend.getPendingExecutorCount === 1)
  }

  test("applicationId returns correct app ID") {
    val appId = backend.applicationId()
    assert(appId === "test-app-123")
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

    // All threads should get the same ID
    val uniqueIds = results.values().toArray.toSet
    assert(uniqueIds.size === 1, s"All threads should get same ID, got: $uniqueIds")
  }

  test("getAuthToken returns None when getAuthToken.sh script is not present") {
    val token = org.apache.spark.deploy.armada.submit.ArmadaUtils.getAuthToken()

    assert(token === None)
  }
}
