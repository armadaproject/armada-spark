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

import org.apache.spark.internal.config.Network.{NETWORK_TIMEOUT, RPC_ASK_TIMEOUT}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.ManualClock
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.mockito.ArgumentMatchers.anyString
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ArmadaClusterSchedulerBackendSuite

    extends AnyFunSuite with BeforeAndAfter {

  @Mock
  private var sc: SparkContext = _

  @Mock
  private var env: SparkEnv = _

  @Mock
  private var taskSchedulerImpl: TaskSchedulerImpl = _

  @Mock
  private var rpcEnv: RpcEnv = _

  private val timeout = 10000
  private val sparkConf = new SparkConf(false)
    .set("spark.armada.executor.trackerTimeout", timeout.toString)

  before {
    MockitoAnnotations.openMocks(this).close()
    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(env)
    when(taskSchedulerImpl.sc).thenReturn(sc)
    when(env.rpcEnv).thenReturn(rpcEnv)
    when(taskSchedulerImpl.isExecutorAlive("1")).thenReturn(true)
  }
  def runTrackerTest(): Unit = {
    val clock = new ManualClock()
    val backend = new ArmadaClusterSchedulerBackend(
      taskSchedulerImpl, sc, null, "master"
    )
    val executorTracker = new backend.ExecutorTracker(clock, 2)
    clock.advance(timeout - 1)
    executorTracker.checkMin()
    verify(taskSchedulerImpl, never()).error(anyString())
    clock.advance(timeout + 1)
    executorTracker.checkMin()
  }
  test("Verify ExecutorTracker discovers insufficient executor") {
    when(taskSchedulerImpl.isExecutorAlive("2")).thenReturn(false)
    runTrackerTest()
    verify(taskSchedulerImpl).error(anyString())
  }

  test("Verify ExecutorTracker no errors on sufficient executor") {
    when(taskSchedulerImpl.isExecutorAlive("2")).thenReturn(true)
    runTrackerTest()
    verify(taskSchedulerImpl, never()).error(anyString())
  }
}
