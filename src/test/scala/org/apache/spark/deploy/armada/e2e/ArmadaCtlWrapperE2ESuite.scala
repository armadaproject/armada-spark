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

import java.util.UUID
import scala.util.{Failure, Success}

class ArmadaCtlWrapperE2ESuite extends AnyFunSuite with Matchers {

  private val wrapper = new ArmadaCtlWrapper()

  test("Create and delete queue using Scala", E2ETest) {
    val testQueue = s"e2e-test-${UUID.randomUUID().toString.take(8)}"

    wrapper.createQueue(testQueue) match {
      case Success(_) =>
      case Failure(exception) =>
        fail(s"Failed to create queue $testQueue: ${exception.getMessage}")
    }

    wrapper.getQueue(testQueue) match {
      case Success(queue) =>
        queue.name shouldBe testQueue
        queue.priorityFactor shouldBe 1.0
      case Failure(exception) =>
        fail(s"Queue verification failed: ${exception.getMessage}")
    }

    wrapper.deleteQueue(testQueue) match {
      case Success(_) =>
        wrapper.getQueue(testQueue) match {
          case Success(_) => fail(s"Queue $testQueue still exists after deletion")
          case Failure(_) =>
        }
      case Failure(exception) =>
        fail(s"Failed to delete queue $testQueue: ${exception.getMessage}")
    }
  }

  test("parseQueueYaml should handle various field formats and missing fields", E2ETest) {
    val basicYaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: test-queue
priorityFactor: 1.5"""

    wrapper.parseQueueYaml(basicYaml) match {
      case Success(queue) =>
        queue.name shouldBe "test-queue"
        queue.priorityFactor shouldBe 1.5
        queue.cordoned shouldBe false
        queue.permissions shouldBe empty
        queue.labels shouldBe empty
      case Failure(ex) => fail(s"Failed to parse basic queue: ${ex.getMessage}")
    }

    val minimalYaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: minimal-queue"""

    wrapper.parseQueueYaml(minimalYaml) match {
      case Success(queue) =>
        queue.name shouldBe "minimal-queue"
        queue.priorityFactor shouldBe 0.0
        queue.cordoned shouldBe false
        queue.permissions shouldBe empty
        queue.labels shouldBe empty
      case Failure(ex) => fail(s"Failed to parse minimal queue: ${ex.getMessage}")
    }

    val stringFieldsYaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: string-fields-queue
priorityFactor: "3.14"
cordoned: "true""""

    wrapper.parseQueueYaml(stringFieldsYaml) match {
      case Success(queue) =>
        queue.name shouldBe "string-fields-queue"
        queue.priorityFactor shouldBe 3.14
        queue.cordoned shouldBe true
      case Failure(ex) => fail(s"Failed to parse queue with string fields: ${ex.getMessage}")
    }

    val emptyCollectionsYaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: empty-collections-queue
permissions: []
labels: {}"""

    wrapper.parseQueueYaml(emptyCollectionsYaml) match {
      case Success(queue) =>
        queue.name shouldBe "empty-collections-queue"
        queue.permissions shouldBe empty
        queue.labels shouldBe empty
      case Failure(ex) => fail(s"Failed to parse queue with empty collections: ${ex.getMessage}")
    }
  }

  test("parseQueueYaml should parse queue with all fields", E2ETest) {
    val yaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: full-queue
priorityFactor: 2.0
cordoned: true
permissions:
- subjects:
  - kind: User
    name: alice
  - kind: Group
    name: developers
  verbs:
  - submit
  - cancel
  - watch
- subjects:
  - kind: User
    name: bob
  verbs:
  - watch
labels:
  environment: production
  team: data-platform"""

    wrapper.parseQueueYaml(yaml) match {
      case Success(queue) =>
        queue.name shouldBe "full-queue"
        queue.priorityFactor shouldBe 2.0
        queue.cordoned shouldBe true

        queue.permissions should have size 2

        val firstPerm = queue.permissions.head
        firstPerm.subjects should have size 2
        firstPerm.subjects.head.kind shouldBe "User"
        firstPerm.subjects.head.name shouldBe "alice"
        firstPerm.subjects(1).kind shouldBe "Group"
        firstPerm.subjects(1).name shouldBe "developers"
        firstPerm.verbs should contain theSameElementsAs Seq("submit", "cancel", "watch")

        val secondPerm = queue.permissions(1)
        secondPerm.subjects should have size 1
        secondPerm.subjects.head.kind shouldBe "User"
        secondPerm.subjects.head.name shouldBe "bob"
        secondPerm.verbs should contain("watch")

        queue.labels should have size 2
        queue.labels("environment") shouldBe "production"
        queue.labels("team") shouldBe "data-platform"

      case Failure(ex) => fail(s"Failed to parse full queue: ${ex.getMessage}")
    }
  }

  test("parseQueueYaml should handle edge cases in permissions", E2ETest) {
    val emptySubjectsYaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: empty-subjects-queue
permissions:
- subjects: []
  verbs:
  - submit"""

    wrapper.parseQueueYaml(emptySubjectsYaml) match {
      case Success(queue) =>
        queue.name shouldBe "empty-subjects-queue"
        queue.permissions should have size 1
        queue.permissions.head.subjects shouldBe empty
        queue.permissions.head.verbs should contain("submit")
      case Failure(ex) => fail(s"Failed to parse queue with empty subjects: ${ex.getMessage}")
    }

    val emptyVerbsYaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: empty-verbs-queue
permissions:
- subjects:
  - kind: User
    name: alice
  verbs: []"""

    wrapper.parseQueueYaml(emptyVerbsYaml) match {
      case Success(queue) =>
        queue.name shouldBe "empty-verbs-queue"
        queue.permissions should have size 1
        queue.permissions.head.subjects should have size 1
        queue.permissions.head.subjects.head.name shouldBe "alice"
        queue.permissions.head.verbs shouldBe empty
      case Failure(ex) => fail(s"Failed to parse queue with empty verbs: ${ex.getMessage}")
    }
  }

  test("parseQueueYaml should handle invalid YAML", E2ETest) {
    val invalidYaml = """this is not valid yaml: [[["""

    wrapper.parseQueueYaml(invalidYaml) match {
      case Success(_)  => fail("Should have failed to parse invalid YAML")
      case Failure(ex) => ex.getMessage should include("yaml")
    }
  }

  test("parseQueueYaml should handle malformed priorityFactor gracefully", E2ETest) {
    val yaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: bad-priority-queue
priorityFactor: "not-a-number""""

    wrapper.parseQueueYaml(yaml) match {
      case Success(_)  => fail("Should have failed to parse invalid priorityFactor")
      case Failure(ex) => ex.getMessage should include("not a valid `double` value")
    }
  }

  test("parseQueueYaml should handle real armadactl output format", E2ETest) {
    val realYaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: test
permissions:
- subjects:
  - kind: User
    name: anonymous
  verbs:
  - submit
  - cancel
  - preempt
  - reprioritize
  - watch
priorityFactor: 1"""

    wrapper.parseQueueYaml(realYaml) match {
      case Success(queue) =>
        queue.name shouldBe "test"
        queue.priorityFactor shouldBe 1.0
        queue.cordoned shouldBe false
        queue.permissions should have size 1

        val perm = queue.permissions.head
        perm.subjects should have size 1
        perm.subjects.head.kind shouldBe "User"
        perm.subjects.head.name shouldBe "anonymous"
        perm.verbs should contain theSameElementsAs Seq(
          "submit",
          "cancel",
          "preempt",
          "reprioritize",
          "watch"
        )

        queue.labels shouldBe empty
      case Failure(ex) => fail(s"Failed to parse real armadactl output: ${ex.getMessage}")
    }
  }

  test("parseQueueYaml should preserve field order independence", E2ETest) {
    val yaml = """priorityFactor: 2.5
permissions:
- verbs:
  - submit
  subjects:
  - name: bob
    kind: User
apiVersion: armadaproject.io/v1beta1
cordoned: false
name: reordered-queue
kind: Queue"""

    wrapper.parseQueueYaml(yaml) match {
      case Success(queue) =>
        queue.name shouldBe "reordered-queue"
        queue.priorityFactor shouldBe 2.5
        queue.cordoned shouldBe false
        queue.permissions should have size 1
        queue.permissions.head.subjects.head.name shouldBe "bob"
        queue.permissions.head.verbs should contain("submit")
      case Failure(ex) => fail(s"Failed to parse reordered YAML: ${ex.getMessage}")
    }
  }

  test("parseQueueYaml should handle complex nested permissions", E2ETest) {
    val yaml = """apiVersion: armadaproject.io/v1beta1
kind: Queue
name: complex-perms-queue
permissions:
- subjects:
  - kind: User
    name: admin
  - kind: Group
    name: administrators
  - kind: ServiceAccount
    name: cluster-admin
  verbs:
  - submit
  - cancel
  - preempt
  - reprioritize
  - watch
- subjects:
  - kind: User
    name: readonly
  verbs:
  - watch"""

    wrapper.parseQueueYaml(yaml) match {
      case Success(queue) =>
        queue.name shouldBe "complex-perms-queue"
        queue.permissions should have size 2

        // First permission
        val adminPerm = queue.permissions.head
        adminPerm.subjects should have size 3
        adminPerm.subjects.map(_.kind) should contain theSameElementsAs Seq(
          "User",
          "Group",
          "ServiceAccount"
        )
        adminPerm.subjects.map(_.name) should contain theSameElementsAs Seq(
          "admin",
          "administrators",
          "cluster-admin"
        )
        adminPerm.verbs should have size 5

        // Second permission
        val readonlyPerm = queue.permissions(1)
        readonlyPerm.subjects should have size 1
        readonlyPerm.subjects.head.kind shouldBe "User"
        readonlyPerm.subjects.head.name shouldBe "readonly"
        readonlyPerm.verbs should contain only "watch"

      case Failure(ex) => fail(s"Failed to parse complex permissions: ${ex.getMessage}")
    }
  }
}
