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

  /** Regression test for the gang-cardinality vs batchSize mismatch. Pre-capture, Armada enforces
    * the gang cardinality across the bootstrap batch — submitting more than that causes every
    * job past the cardinality to be rejected with "cannot submit more jobs to gang than
    * specified in gang cardinality", which the allocator then retries indefinitely. The
    * allocator must consult backend.getGangCardinality during each tick so the cap can be
    * applied.
    */
  test("tryAllocateExecutors consults gangCardinality before submitting") {
    val backend = mock(classOf[ArmadaClusterManagerBackend])
    // Drive the loop into the submit branch: gap > 0 and isInitialBatch=true.
    when(backend.getExecutorCounts).thenReturn((0, 0))
    when(backend.isReadyToAllocateMore).thenReturn(true)
    // Cardinality < batchSize would otherwise overflow the gang. Returning 1 exercises the cap.
    when(backend.getGangCardinality).thenReturn(1)

    val armadaClient = mock(classOf[ArmadaClient])
    val conf = new SparkConf(false)
      .set("spark.armada.allocation.batchSize", "4")

    val allocator = new ArmadaExecutorAllocator(
      armadaClient,
      "test-queue",
      "test-jobset",
      conf,
      "test-app",
      backend
    )

    // Register a target so the allocator's loop body executes.
    val rp = org.apache.spark.resource.ResourceProfile.getOrCreateDefaultProfile(conf)
    allocator.setTotalExpectedExecutors(Map(rp -> 4))

    val method = classOf[ArmadaExecutorAllocator].getDeclaredMethod("tryAllocateExecutors")
    method.setAccessible(true)
    method.invoke(allocator)

    // The cap is read on every submission-eligible tick. Without the cap, the allocator would
    // submit batchSize=4 jobs and overflow Armada's cardinality=1 gang.
    verify(backend, atLeastOnce()).getGangCardinality
  }
}
