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
import org.apache.spark.deploy.armada.Config
import org.apache.spark.deploy.armada.submit.GangSchedulingAnnotations
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}

case class PodSnapshot(allPods: Seq[Pod], namespace: String) {
  def filterByLabels(labels: Map[String, String]): Seq[Pod] =
    allPods.filter { pod =>
      val podLabels = Option(pod.getMetadata.getLabels)
        .map(_.asScala.toMap)
        .getOrElse(Map.empty)
      labels.forall { case (k, v) => podLabels.get(k).contains(v) }
    }
}

trait TestAssertion {
  def name: String
  def assert(context: AssertionContext)(implicit ec: ExecutionContext): Future[AssertionResult]
}

case class AssertionContext(
    namespace: String,
    queueName: String,
    testId: String,
    k8sClient: K8sClient,
    armadaClient: ArmadaClient,
    podSnapshot: Option[PodSnapshot] = None
) {

  /** Get pods by label - uses snapshot if available, else hits API */
  def getPodsByLabel(
      labelSelector: String
  )(implicit ec: ExecutionContext): Future[Seq[Pod]] =
    podSnapshot match {
      case Some(snapshot) =>
        val labels = Config.commaSeparatedLabelsToMap(labelSelector)
        Future.successful(snapshot.filterByLabels(labels))
      case None =>
        k8sClient.getPodsByLabel(labelSelector, namespace)
    }

  /** Get pods with their node selectors - uses snapshot if available */
  def getPodsWithNodeSelectors(
      labelSelector: String
  )(implicit ec: ExecutionContext): Future[Seq[(String, Map[String, String])]] =
    podSnapshot match {
      case Some(snapshot) =>
        val labels = Config.commaSeparatedLabelsToMap(labelSelector)
        Future.successful(snapshot.filterByLabels(labels).map { pod =>
          val name = pod.getMetadata.getName
          val ns = Option(pod.getSpec.getNodeSelector)
            .map(_.asScala.toMap)
            .getOrElse(Map.empty)
          (name, ns)
        })
      case None =>
        k8sClient.getPodsWithNodeSelectors(labelSelector, namespace)
    }
}

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
    context.getPodsByLabel(labelSelector).map { pods =>
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
    context.getPodsByLabel(labelSelector).map { pods =>
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

/** Tracks all pods seen across polls and verifies each has gang annotations (GANG_ID and
  * GANG_NODE_UNIFORMITY_LABEL). Use for dynamic allocation where pods come and go.
  */
class DynamicGangAnnotationAssertion(
    nodeUniformityLabel: String,
    expectedMinPods: Int,
    podRole: String = "executor"
) extends TestAssertion {
  override val name =
    s"At least $expectedMinPods $podRole pods should have gang annotations ($nodeUniformityLabel)"

  // pod name -> whether it passed validation
  private val seenPods = scala.collection.mutable.Map.empty[String, Boolean]

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=$podRole,test-id=${context.testId}"
    context.getPodsByLabel(labelSelector).map { pods =>
      pods.foreach { pod =>
        val name = pod.getMetadata.getName
        if (!seenPods.contains(name)) {
          val annotations = pod.getMetadata.getAnnotations
          val valid = annotations != null &&
            Option(annotations.get(GangSchedulingAnnotations.GANG_ID)).exists(_.nonEmpty) &&
            Option(annotations.get(GangSchedulingAnnotations.GANG_NODE_UNIFORMITY_LABEL))
              .contains(nodeUniformityLabel)
          seenPods(name) = valid
        }
      }

      val validCount = seenPods.count(_._2)
      val failed     = seenPods.filter(!_._2).keys.toSeq

      if (failed.nonEmpty) {
        AssertionResult.Failure(
          s"${failed.size} $podRole pod(s) missing gang annotations: ${failed.mkString(", ")}"
        )
      } else if (validCount >= expectedMinPods) {
        println(s"Seen $validCount valid $podRole pod(s)")
        AssertionResult.Success
      } else {
        AssertionResult.Failure(
          s"Only seen $validCount valid $podRole pod(s) so far, need at least $expectedMinPods"
        )
      }
    }
  }
}

/** Tracks all executor pods seen across polls and verifies each has expected labels. Use for
  * dynamic allocation where pods come and go.
  */
class DynamicExecutorLabelAssertion(
    expectedLabels: Map[String, String],
    expectedMinPods: Int
) extends TestAssertion {
  override val name =
    s"At least $expectedMinPods executor pods should have labels $expectedLabels"

  private val seenPods = scala.collection.mutable.Map.empty[String, Boolean]

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=executor,test-id=${context.testId}"
    context.getPodsByLabel(labelSelector).map { pods =>
      pods.foreach { pod =>
        val name = pod.getMetadata.getName
        if (!seenPods.contains(name)) {
          val podLabels = Option(pod.getMetadata.getLabels)
            .map(_.asScala.toMap)
            .getOrElse(Map.empty)
          val valid = expectedLabels.forall { case (k, v) =>
            podLabels.get(k).contains(v)
          }
          seenPods(name) = valid
        }
      }

      val validCount = seenPods.count(_._2)
      val failed     = seenPods.filter(!_._2).keys.toSeq

      if (failed.nonEmpty) {
        AssertionResult.Failure(
          s"${failed.size} executor pod(s) missing labels: ${failed.mkString(", ")}"
        )
      } else if (validCount >= expectedMinPods) {
        println(s"Seen $validCount valid executor pod(s) with expected labels")
        AssertionResult.Success
      } else {
        AssertionResult.Failure(
          s"Only seen $validCount valid executor pod(s) so far, need at least $expectedMinPods"
        )
      }
    }
  }
}

/** Tracks all executor pods seen across polls and verifies each has expected annotations. Use for
  * dynamic allocation where pods come and go.
  */
class DynamicExecutorAnnotationAssertion(
    expectedAnnotations: Map[String, String],
    expectedMinPods: Int
) extends TestAssertion {
  override val name =
    s"At least $expectedMinPods executor pods should have annotations $expectedAnnotations"

  private val seenPods = scala.collection.mutable.Map.empty[String, Boolean]

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=executor,test-id=${context.testId}"
    context.getPodsByLabel(labelSelector).map { pods =>
      pods.foreach { pod =>
        val name = pod.getMetadata.getName
        if (!seenPods.contains(name)) {
          val podAnnotations = Option(pod.getMetadata.getAnnotations)
            .map(_.asScala.toMap)
            .getOrElse(Map.empty)
          val valid = expectedAnnotations.forall { case (k, v) =>
            podAnnotations.get(k).contains(v)
          }
          seenPods(name) = valid
        }
      }

      val validCount = seenPods.count(_._2)
      val failed     = seenPods.filter(!_._2).keys.toSeq

      if (failed.nonEmpty) {
        AssertionResult.Failure(
          s"${failed.size} executor pod(s) missing annotations: ${failed.mkString(", ")}"
        )
      } else if (validCount >= expectedMinPods) {
        println(s"Seen $validCount valid executor pod(s) with expected annotations")
        AssertionResult.Success
      } else {
        AssertionResult.Failure(
          s"Only seen $validCount valid executor pod(s) so far, need at least $expectedMinPods"
        )
      }
    }
  }
}

/** Tracks all executor pods seen across polls and verifies each passes a predicate. Use for dynamic
  * allocation where pods come and go.
  */
class DynamicExecutorPodAssertion(
    predicate: Pod => Boolean,
    predicateDescription: String,
    expectedMinPods: Int
) extends TestAssertion {
  override val name =
    s"At least $expectedMinPods executor pods should satisfy: $predicateDescription"

  private val seenPods = scala.collection.mutable.Map.empty[String, Boolean]

  override def assert(
      context: AssertionContext
  )(implicit ec: ExecutionContext): Future[AssertionResult] = {
    val labelSelector = s"spark-role=executor,test-id=${context.testId}"
    context.getPodsByLabel(labelSelector).map { pods =>
      pods.foreach { pod =>
        val name = pod.getMetadata.getName
        if (!seenPods.contains(name)) {
          seenPods(name) = predicate(pod)
        }
      }

      val validCount = seenPods.count(_._2)
      val failed     = seenPods.filter(!_._2).keys.toSeq

      if (failed.nonEmpty) {
        AssertionResult.Failure(
          s"${failed.size} executor pod(s) failed $predicateDescription: ${failed.mkString(", ")}"
        )
      } else if (validCount >= expectedMinPods) {
        println(
          s"Seen $validCount valid executor pod(s) satisfying: $predicateDescription"
        )
        AssertionResult.Success
      } else {
        AssertionResult.Failure(
          s"Only seen $validCount valid executor pod(s) so far, need at least $expectedMinPods"
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
    context.getPodsByLabel(podSelector).map { pods =>
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
    context.getPodsWithNodeSelectors(podSelector).map { pods =>
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
    context.getPodsByLabel(podSelector).map { pods =>
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
    for {
      // Find driver pod using test-id label in the test namespace
      pods <- context.getPodsByLabel(s"test-id=${context.testId}")
      podName = pods.headOption
        .getOrElse(throw new AssertionError("Driver pod not found"))
        .getMetadata
        .getName
      ingress <- context.k8sClient.getIngressForPod(podName, context.namespace)
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
    context.getPodsByLabel(podSelector).map { pods =>
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
