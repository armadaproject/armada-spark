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

import io.armadaproject.armada.ArmadaClient

import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkConf

class ArmadaExecutorAllocatorSuite extends AnyFunSuite with Matchers {

  test("tryAllocateExecutors is a no-op when backend is stopping") {
    val backend = mock(classOf[ArmadaClusterManagerBackend])
    when(backend.isStopping).thenReturn(true)

    val armadaClient = mock(classOf[ArmadaClient])
    val conf         = new SparkConf(false)

    val allocator = new ArmadaExecutorAllocator(
      armadaClient,
      "test-queue",
      "test-jobset",
      conf,
      "test-app",
      backend
    )

    val method = classOf[ArmadaExecutorAllocator].getDeclaredMethod("tryAllocateExecutors")
    method.setAccessible(true)
    method.invoke(allocator)

    verify(backend, never()).getExecutorCounts
    verify(backend, never()).recordAndPendExecutor(org.mockito.ArgumentMatchers.anyString())
  }
}
