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

import io.fabric8.kubernetes.api.model.Pod
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}

trait TestAssertion {
  def name: String
  def assert(context: AssertionContext)(implicit ec: ExecutionContext): Future[AssertionResult]
}

case class AssertionContext(
    namespace: String,
    queueName: String,
    testId: String,
    k8sClient: K8sClient,
    armadaClient: ArmadaClient
)

sealed trait AssertionResult
object AssertionResult {
  case object Success                 extends AssertionResult
  case class Failure(message: String) extends AssertionResult
}

/** Pod count assertion - verifies expected number of pods with given labels */
class PodCountAssertion(
    labelSelector: String,
    expectedCount: Int,
    description: String = "Pod count"
) extends TestAssertion {
  override val name = s"$description should be $expectedCount"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    k8sClient.getPodsByLabel(labelSelector, namespace).map { pods =>
      if (pods.size == expectedCount) {
        AssertionResult.Success
      } else {
        AssertionResult.Failure(
          s"Expected $expectedCount pods with selector '$labelSelector', found ${pods.size}"
        )
      }
    }
  }
}

/** Driver exists assertion - verifies driver pod exists */
class DriverExistsAssertion extends TestAssertion {
  override val name = "Driver pod exists"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=driver,test-id=${context.testId}"
    new PodCountAssertion(labelSelector, 1, "Driver pods").assert(context)
  }
}

/** Executor count assertion - verifies exact number of executor pods */
class ExecutorCountAssertion(expectedCount: Int) extends TestAssertion {
  override val name = s"Executor count should be $expectedCount"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=executor,test-id=${context.testId}"
    new PodCountAssertion(labelSelector, expectedCount, "Executor pods").assert(context)
  }
}

class ExecutorCountMaxReachedAssertion(expectedMinMax: Int) extends TestAssertion {
  override val name = s"Executor count max should have reached at least $expectedMinMax"

  private var maxSeen: Int = 0

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=executor,test-id=${context.testId}"
    context.k8sClient.getPodsByLabel(labelSelector, context.namespace).map { pods =>
      val count = pods.size
      maxSeen = math.max(maxSeen, count)
      if (maxSeen >= expectedMinMax) {
        println(s"Executor count max reached $expectedMinMax (max seen: $maxSeen, current: $count)")
        AssertionResult.Success
      } else {
        AssertionResult.Failure(
          s"Executor count max never reached $expectedMinMax (max seen: $maxSeen, current: $count)"
        )
      }
    }
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

    k8sClient.getPodsByLabel(podSelector, namespace).map { pods =>
      pods.headOption match {
        case None =>
          AssertionResult.Failure(s"No pod found with selector: $podSelector")
        case Some(pod) =>
          val podLabels = pod.getMetadata.getLabels.asScala.toMap
          val missingLabels = expectedLabels.filter { case (key, value) =>
            !podLabels.get(key).contains(value)
          }
          if (missingLabels.isEmpty) {
            AssertionResult.Success
          } else {
            AssertionResult.Failure(s"Missing or incorrect labels: $missingLabels")
          }
      }
    }
  }
}

/** Driver label assertion - verifies driver pod has expected labels */
class DriverLabelAssertion(expectedLabels: Map[String, String]) extends TestAssertion {
  override val name = "Driver labels"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=driver,test-id=${context.testId}"
    new PodLabelAssertion(selector, expectedLabels).assert(context)
  }
}

/** Executor label assertion - verifies executor pods have expected labels */
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
          val podAnnotations = pod.getMetadata.getAnnotations.asScala.toMap
          val missingAnnotations = expectedAnnotations.filter { case (key, value) =>
            !podAnnotations.get(key).contains(value)
          }
          if (missingAnnotations.nonEmpty) {
            Some(
              s"Pod ${pod.getMetadata.getName} missing or incorrect annotations: $missingAnnotations"
            )
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

/** Driver annotation assertion - verifies driver pod has expected annotations */
class DriverAnnotationAssertion(expectedAnnotations: Map[String, String]) extends TestAssertion {
  override val name = "Driver annotations"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=driver,test-id=${context.testId}"
    new PodAnnotationAssertion(selector, expectedAnnotations).assert(context)
  }
}

class ExecutorAnnotationAssertion(expectedAnnotations: Map[String, String]) extends TestAssertion {
  override val name = "Executor annotations"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=executor,test-id=${context.testId}"
    new PodAnnotationAssertion(selector, expectedAnnotations).assert(context)
  }
}

/** Ingress assertion - verifies ingress for driver pod exists and has expected annotations and port
  */
class IngressAssertion(
    requiredAnnotations: Map[String, String],
    requiredPort: Int = 4040
) extends TestAssertion {

  override val name = "Ingress annotations verification"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    for {
      // Find driver pod using test-id label in the test namespace
      pods <- k8sClient.getPodsByLabel(s"test-id=$testId", namespace)
      podName = pods.headOption
        .getOrElse(throw new AssertionError("Driver pod not found"))
        .getMetadata
        .getName
      ingress <- k8sClient.getIngressForPod(podName, namespace)
    } yield {
      ingress match {
        case None =>
          AssertionResult.Failure(s"No ingress found for driver pod: $podName")
        case Some(ing) =>
          val ingressAnnotations = ing.getMetadata.getAnnotations.asScala.toMap
          val missingOrIncorrect = requiredAnnotations.filter { case (key, value) =>
            !ingressAnnotations.get(key).contains(value)
          }
          if (missingOrIncorrect.nonEmpty) {
            AssertionResult.Failure(
              s"Missing or incorrect ingress annotations: $missingOrIncorrect"
            )
          } else {
            // Check if required port exists in any rule's paths
            val hasRequiredPort = ing.getSpec.getRules.asScala.exists { rule =>
              rule.getHttp.getPaths.asScala.exists { path =>
                path.getBackend.getService.getPort.getNumber == requiredPort
              }
            }

            if (hasRequiredPort) {
              AssertionResult.Success
            } else {
              AssertionResult.Failure(s"Expected port $requiredPort not found in ingress")
            }
          }
      }
    }
  }
}

/** Driver pod assertion with custom predicate */
class DriverPodAssertion(
    predicate: Pod => Boolean
) extends TestAssertion {
  override val name = "Driver pod assertion"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=driver,test-id=${context.testId}"
    new GenericPodAssertion(
      selector,
      predicate,
      pod => s"Driver pod ${pod.getMetadata.getName} failed assertion"
    ).assert(context)
  }
}

/** Executor pod assertion with custom predicate */
class ExecutorPodAssertion(
    predicate: Pod => Boolean
) extends TestAssertion {
  override val name = "Executor pod assertion"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val selector = s"spark-role=executor,test-id=${context.testId}"
    new GenericPodAssertion(
      selector,
      predicate,
      pod => s"Executor pod ${pod.getMetadata.getName} failed assertion"
    ).assert(context)
  }
}

/** Generic pod assertion using a predicate function */
class GenericPodAssertion(
    podSelector: String,
    predicate: Pod => Boolean,
    errorMessage: Pod => String
) extends TestAssertion {
  override val name = "Pod assertion"

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    import context._

    k8sClient.getPodsByLabel(podSelector, namespace).map { pods =>
      if (pods.isEmpty) {
        AssertionResult.Failure(s"No pods found with selector: $podSelector")
      } else {
        val failures = pods.filterNot(predicate).map(errorMessage)

        if (failures.isEmpty) {
          AssertionResult.Success
        } else {
          AssertionResult.Failure(failures.mkString("; "))
        }
      }
    }
  }
}
