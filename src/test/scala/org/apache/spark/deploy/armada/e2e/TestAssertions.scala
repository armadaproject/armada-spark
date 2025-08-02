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
    k8sClient: K8sOperations,
    armadaClient: ArmadaOperations
)

sealed trait AssertionResult
object AssertionResult {
  case object Success                                                  extends AssertionResult
  case class Failure(message: String, cause: Option[Throwable] = None) extends AssertionResult
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

class TemplateIngressAssertion
    extends IngressAssertion(
      requiredAnnotations = Set(
        "nginx.ingress.kubernetes.io/rewrite-target",
        "nginx.ingress.kubernetes.io/backend-protocol",
        "kubernetes.io/ingress.class"
      )
    ) {
  override val name = "Template Ingress Verification"
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

  def assertAllSuccess(results: Map[String, AssertionResult]): Unit = {
    val failures = results.collect { case (name, AssertionResult.Failure(msg, cause)) =>
      (name, msg, cause)
    }

    if (failures.nonEmpty) {
      val message = failures
        .map { case (name, msg, _) =>
          s"$name: $msg"
        }
        .mkString("\n")

      throw new AssertionError(s"Assertions failed:\n$message")
    }
  }
}
