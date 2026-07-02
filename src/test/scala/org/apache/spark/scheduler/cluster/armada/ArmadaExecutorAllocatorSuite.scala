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
import org.apache.spark.resource.ResourceProfile

class ArmadaExecutorAllocatorSuite extends AnyFunSuite with Matchers {

  private def newAllocator(
      backend: ArmadaClusterManagerBackend,
      armadaClient: ArmadaClient
  ): ArmadaExecutorAllocator =
    new ArmadaExecutorAllocator(
      armadaClient,
      "test-queue",
      "test-jobset",
      new SparkConf(false),
      "test-app",
      backend
    )

  // Pre-load totalExpectedExecutors so the foreach in tryAllocateExecutors actually iterates —
  // otherwise the assertion below holds vacuously whether or not the isStopping gate exists.
  private def primeDemand(allocator: ArmadaExecutorAllocator): Unit = {
    val rp = ResourceProfile.getOrCreateDefaultProfile(new SparkConf(false))
    allocator.setTotalExpectedExecutors(Map(rp -> 4))
  }

  test("tryAllocateExecutors is a no-op when backend is stopping") {
    val backend = mock(classOf[ArmadaClusterManagerBackend])
    when(backend.isStopping).thenReturn(true)
    val allocator = newAllocator(backend, mock(classOf[ArmadaClient]))
    primeDemand(allocator)

    allocator.tryAllocateExecutors()

    verify(backend, never()).getExecutorCounts
  }

  // Positive control: with the same primed demand but isStopping=false, the allocator must reach
  // backend.getExecutorCounts. Proves the previous test isn't passing vacuously.
  test("tryAllocateExecutors reaches getExecutorCounts when backend is not stopping") {
    val backend = mock(classOf[ArmadaClusterManagerBackend])
    when(backend.isStopping).thenReturn(false)
    when(backend.getExecutorCounts).thenReturn((4, 0))
    val allocator = newAllocator(backend, mock(classOf[ArmadaClient]))
    primeDemand(allocator)

    allocator.tryAllocateExecutors()

    verify(backend, atLeastOnce()).getExecutorCounts
  }

  // submitExecutorJobs is the second leg of the shutdown gate: a tick that already passed the
  // tryAllocateExecutors check must not push a submission through if stopping flips in the
  // meantime. We verify (a) the gate is consulted and (b) the method early-returns without
  // raising — distinguishing it from the not-stopping path which enters the try/catch block and
  // swallows the validation error from an empty SparkConf.
  test("submitExecutorJobs early-returns when backend is stopping") {
    val backend = mock(classOf[ArmadaClusterManagerBackend])
    when(backend.isStopping).thenReturn(true)
    val allocator = newAllocator(backend, mock(classOf[ArmadaClient]))

    allocator.submitExecutorJobs(count = 1, rpId = 0)

    verify(backend, atLeastOnce()).isStopping
    verify(backend, never()).recordAndPendExecutor(org.mockito.ArgumentMatchers.anyString())
  }
}
