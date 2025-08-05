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

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

trait TestAssertion {
  def name: String
  def assert(context: AssertionContext)(implicit ec: ExecutionContext): Future[AssertionResult]
}

case class AssertionContext(
    jobSetId: String,
    namespace: String,
    testId: String,
    k8sClient: K8sClient,
    armadaClient: ArmadaClient
)

sealed trait AssertionResult
object AssertionResult {
  case object Success                                                  extends AssertionResult
  case class Failure(message: String, cause: Option[Throwable] = None) extends AssertionResult
}

/** Pod count assertion - verifies expected number of pods with given labels */
class PodCountAssertion(
    labelSelector: String,
    expectedCount: Int,
    description: String = "Pod count"
) extends TestAssertion {
  override val name = s"$description verification"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    Future {
      val cmd    = Seq("kubectl", "get", "pods", "-n", namespace, "-l", labelSelector, "-o", "name")
      val result = ProcessExecutor.execute(cmd, 10.seconds)

      if (result.exitCode == 0) {
        val podCount = result.stdout.split("\n").filter(_.nonEmpty).length
        if (podCount == expectedCount) {
          AssertionResult.Success
        } else {
          AssertionResult.Failure(s"Expected $expectedCount pods, found $podCount")
        }
      } else {
        AssertionResult.Failure(s"Failed to get pods: ${result.stderr}")
      }
    }
  }
}

/** Driver exists assertion - uses test-id from context */
class DriverExistsAssertion extends TestAssertion {
  override val name = "Driver exists"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=driver,test-id=${context.testId}"
    new PodCountAssertion(labelSelector, 1, "Driver pod").assert(context)
  }
}

/** Executor count assertion - uses test-id from context */
class ExecutorCountAssertion(expectedCount: Int) extends TestAssertion {
  override val name = s"Executor count ($expectedCount)"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=executor,test-id=${context.testId}"
    new PodCountAssertion(labelSelector, expectedCount, s"$expectedCount executors").assert(context)
  }
}

/** Pod label assertion - verifies pods have expected labels */
class PodLabelAssertion(
    podSelector: String,
    expectedLabels: Map[String, String]
) extends TestAssertion {
  override val name = "Pod labels verification"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    k8sClient.getPodByLabel(podSelector, namespace).map {
      case None =>
        AssertionResult.Failure(s"No pod found with selector: $podSelector")
      case Some(pod) =>
        val missingLabels = expectedLabels.filter { case (key, value) =>
          pod.labels.get(key) != Some(value)
        }
        if (missingLabels.isEmpty) {
          AssertionResult.Success
        } else {
          AssertionResult.Failure(s"Missing or incorrect labels: $missingLabels")
        }
    }
  }
}

/** Driver label assertion - uses test-id from context */
class DriverLabelAssertion(expectedLabels: Map[String, String]) extends TestAssertion {
  override val name = "Driver labels"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=driver,test-id=${context.testId}"
    new PodLabelAssertion(selector, expectedLabels).assert(context)
  }
}

/** Executor label assertion - uses test-id from context */
class ExecutorLabelAssertion(expectedLabels: Map[String, String]) extends TestAssertion {
  override val name = "Executor labels"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=executor,test-id=${context.testId}"
    new PodLabelAssertion(selector, expectedLabels).assert(context)
  }
}

/** Node selector assertion - verifies pods are scheduled with correct node selectors */
class NodeSelectorAssertion(
    podSelector: String,
    expectedNodeSelectors: Map[String, String]
) extends TestAssertion {
  override val name = "Node selector verification"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    Future {
      val cmd = Seq(
        "kubectl",
        "get",
        "pods",
        "-n",
        namespace,
        "-l",
        podSelector,
        "-o",
        "jsonpath={range .items[*]}{.metadata.name} {.spec.nodeSelector}{\"\\n\"}{end}"
      )
      val result = ProcessExecutor.execute(cmd, 10.seconds)

      if (result.exitCode == 0) {
        // Parse the output and check node selectors
        val lines = result.stdout.split("\n").filter(_.nonEmpty)
        val failures = lines.flatMap { line =>
          val parts = line.split(" ", 2)
          if (parts.length < 2) {
            Some(s"Pod ${parts(0)} has no node selectors")
          } else {
            // Simple check - in reality would need proper JSON parsing
            val missing = expectedNodeSelectors.filter { case (k, v) =>
              !parts(1).contains(s"$k:$v")
            }
            if (missing.nonEmpty) Some(s"Pod ${parts(0)} missing node selectors: $missing")
            else None
          }
        }

        if (failures.isEmpty) AssertionResult.Success
        else AssertionResult.Failure(failures.mkString("; "))
      } else {
        AssertionResult.Failure(s"Failed to get pods: ${result.stderr}")
      }
    }
  }
}

/** Helper object with convenience methods for creating common assertions */
object Assertions {

  def executorCount(expected: Int)(implicit testId: String): PodCountAssertion = {
    new PodCountAssertion(
      labelSelector = s"spark-role=executor,test-id=$testId",
      expectedCount = expected,
      description = "Executor count"
    )
  }

  def driverExists(implicit testId: String): PodCountAssertion = {
    new PodCountAssertion(
      labelSelector = s"spark-role=driver,test-id=$testId",
      expectedCount = 1,
      description = "Driver pod"
    )
  }

  def podLabels(selector: String, labels: Map[String, String]): PodLabelAssertion = {
    new PodLabelAssertion(selector, labels)
  }

  def executorLabels(labels: Map[String, String])(implicit testId: String): PodLabelAssertion = {
    new PodLabelAssertion(s"spark-role=executor,test-id=$testId", labels)
  }

  def driverLabels(labels: Map[String, String])(implicit testId: String): PodLabelAssertion = {
    new PodLabelAssertion(s"spark-role=driver,test-id=$testId", labels)
  }

  def nodeSelectors(selector: String, selectors: Map[String, String]): NodeSelectorAssertion = {
    new NodeSelectorAssertion(selector, selectors)
  }

  def driverIngress(annotations: Set[String], port: Int = 7078): IngressAssertion = {
    new IngressAssertion(annotations, port)
  }
}

class IngressAssertion(
    requiredAnnotations: Set[String],
    requiredPort: Int = 7078
) extends TestAssertion {

  override val name = "Ingress Verification"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    for {
      // Find driver pod using test-id label in the test namespace
      pod <- k8sClient.getPodByLabel(s"test-id=$testId", namespace)
      podName = pod.getOrElse(throw new AssertionError("Driver pod not found")).name
      ingress <- k8sClient.getIngressForPod(podName, namespace)
    } yield {
      ingress match {
        case None =>
          AssertionResult.Failure(s"No ingress found for driver pod: $podName")
        case Some(ing) =>
          val missingAnnotations = requiredAnnotations -- ing.annotations.keySet
          if (missingAnnotations.nonEmpty) {
            AssertionResult.Failure(s"Missing annotations: ${missingAnnotations.mkString(", ")}")
          } else if (!ing.rules.exists(_.paths.exists(_.backend.servicePort == requiredPort))) {
            AssertionResult.Failure(s"Expected port $requiredPort not found in ingress")
          } else {
            AssertionResult.Success
          }
      }
    }
  }
}

object AssertionRunner {
  def runAssertions(
      assertions: Seq[TestAssertion],
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[Map[String, AssertionResult]] = {
    Future
      .traverse(assertions) { assertion =>
        Future(assertion.name)
          .zip(assertion.assert(context))
          .map { case (name, result) => name -> result }
          .recover { case ex: Throwable =>
            assertion.name -> AssertionResult.Failure(ex.getMessage, Some(ex))
          }
      }
      .map(_.toMap)
  }
}
