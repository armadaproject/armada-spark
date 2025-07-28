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

package org.apache.spark.deploy.armada.e2e

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class ArmadaClientE2ESuite extends AnyFunSuite with Matchers with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span(30, Seconds),
    interval = Span(1, Seconds)
  )

  private val client = new ArmadaClient()

  test("Create and delete queue using ArmadaClient", E2ETest) {
    val testQueue = s"e2e-test-${UUID.randomUUID().toString.take(8)}"

    val result = for {
      _     <- client.createQueue(testQueue)
      queue <- client.getQueue(testQueue)
      _ = {
        queue shouldBe defined
        queue.get.name shouldBe testQueue
        queue.get.priorityFactor shouldBe 1.0
      }
      _ <- client.createQueue(testQueue) // Should not fail on existing queue
      _ <- client.getQueue(testQueue) map { q =>
        q shouldBe defined
      }
    } yield ()

    result.futureValue
  }

  test("Queue operations should handle non-existent queues", E2ETest) {
    val nonExistentQueue = s"non-existent-${UUID.randomUUID().toString.take(8)}"

    client.getQueue(nonExistentQueue).futureValue shouldBe None
  }

  test("Ensure queue should create if missing", E2ETest) {
    val testQueue = s"ensure-test-${UUID.randomUUID().toString.take(8)}"

    val result = for {
      _     <- client.ensureQueue(testQueue)
      queue <- client.getQueue(testQueue)
    } yield {
      queue shouldBe defined
      queue.get.name shouldBe testQueue
    }

    result.futureValue
  }
}
