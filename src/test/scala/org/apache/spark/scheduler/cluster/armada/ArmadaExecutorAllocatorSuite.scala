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
import org.scalatest.prop.TableDrivenPropertyChecks

import org.apache.spark.SparkConf

class ArmadaExecutorAllocatorSuite
    extends AnyFunSuite
    with Matchers
    with TableDrivenPropertyChecks {

  private def newAllocator(batchSize: Int): ArmadaExecutorAllocator = {
    val backend      = mock(classOf[ArmadaClusterManagerBackend])
    val armadaClient = mock(classOf[ArmadaClient])
    val conf = new SparkConf(false)
      .set("spark.armada.allocation.batchSize", batchSize.toString)
    new ArmadaExecutorAllocator(
      armadaClient,
      "test-queue",
      "test-jobset",
      conf,
      "test-app",
      backend
    )
  }

  test("computeToAllocate caps the batch by gang cardinality") {
    val allocator = newAllocator(batchSize = 4)

    // (gap, gangCardinality, expectedToAllocate)
    val cases = Table(
      ("gap", "gangCardinality", "expected"),
      // gang smaller than batch -> capped to the gang size (the bug this fix targets)
      (4, 1, 1),
      (4, 2, 2),
      // gang larger than batch -> batch size wins
      (4, 10, 4),
      // gang equals batch -> batch size
      (4, 4, 4),
      // no gang constraint (<= 0) -> fall back to batch size
      (4, 0, 4),
      (4, -1, 4),
      // gap smaller than the effective batch -> gap wins
      (2, 4, 2),
      (1, 10, 1),
      (3, 5, 3),
      // gap = 0 -> never submit, regardless of cardinality
      (0, 5, 0)
    )

    forAll(cases) { (gap: Int, gangCardinality: Int, expected: Int) =>
      allocator.computeToAllocate(gap, gangCardinality) shouldBe expected
    }
  }
}
