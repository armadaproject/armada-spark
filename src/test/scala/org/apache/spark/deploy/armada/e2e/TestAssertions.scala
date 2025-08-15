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

    k8sClient.getPodsByLabel(labelSelector, namespace).map { pods =>
      val podCount = pods.size
      if (podCount == expectedCount) {
        AssertionResult.Success
      } else {
        AssertionResult.Failure(s"Expected $expectedCount pods, found $podCount")
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
          !pod.labels.get(key).contains(value)
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

    k8sClient.getPodsWithNodeSelectors(podSelector, namespace).map { pods =>
      if (pods.isEmpty) {
        AssertionResult.Failure(s"No pods found with selector: $podSelector")
      } else {
        val failures = pods.flatMap { case (podName, nodeSelectors) =>
          val missing = expectedNodeSelectors.filter { case (k, v) =>
            !nodeSelectors.get(k).contains(v)
          }
          if (missing.nonEmpty) {
            Some(s"Pod $podName missing node selectors: $missing")
          } else {
            None
          }
        }

        if (failures.isEmpty) {
          AssertionResult.Success
        } else {
          AssertionResult.Failure(failures.mkString("; "))
        }
      }
    }
  }
}

/** Pod annotation assertion - verifies pods have expected annotations */
class PodAnnotationAssertion(
    podSelector: String,
    expectedAnnotations: Map[String, String]
) extends TestAssertion {
  override val name = "Pod annotations verification"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    k8sClient.getPodsByLabel(podSelector, namespace).map { pods =>
      if (pods.isEmpty) {
        AssertionResult.Failure(s"No pods found with selector: $podSelector")
      } else {
        val failures = pods.flatMap { pod =>
          val missingAnnotations = expectedAnnotations.filter { case (key, value) =>
            !pod.annotations.get(key).contains(value)
          }
          if (missingAnnotations.nonEmpty) {
            Some(s"Pod ${pod.name} missing or incorrect annotations: $missingAnnotations")
          } else {
            None
          }
        }

        if (failures.isEmpty) {
          AssertionResult.Success
        } else {
          AssertionResult.Failure(failures.mkString("; "))
        }
      }
    }
  }
}

/** Driver annotation assertion - uses test-id from context */
class DriverAnnotationAssertion(expectedAnnotations: Map[String, String]) extends TestAssertion {
  override val name = "Driver annotations"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=driver,test-id=${context.testId}"
    new PodAnnotationAssertion(selector, expectedAnnotations).assert(context)
  }
}

/** Executor annotation assertion - uses test-id from context */
class ExecutorAnnotationAssertion(expectedAnnotations: Map[String, String]) extends TestAssertion {
  override val name = "Executor annotations"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=executor,test-id=${context.testId}"
    new PodAnnotationAssertion(selector, expectedAnnotations).assert(context)
  }
}

class IngressAssertion(
    requiredAnnotations: Map[String, String],
    requiredPort: Int = 7078
) extends TestAssertion {

  override val name = "Ingress annotations verification"

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
          val missingOrIncorrect = requiredAnnotations.filter { case (key, value) =>
            !ing.annotations.get(key).contains(value)
          }
          if (missingOrIncorrect.nonEmpty) {
            AssertionResult.Failure(
              s"Missing or incorrect ingress annotations: $missingOrIncorrect"
            )
          } else if (!ing.rules.exists(_.paths.exists(_.backend.servicePort == requiredPort))) {
            AssertionResult.Failure(s"Expected port $requiredPort not found in ingress")
          } else {
            AssertionResult.Success
          }
      }
    }
  }
}
