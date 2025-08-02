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

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.mutable.ArrayBuffer

/** Abstraction for Kubernetes operations used in E2E testing.
  *
  * Provides async methods for querying and waiting for Kubernetes resources. All operations are
  * non-blocking and return Futures for composition.
  */
trait K8sOperations {

  /** Creates a namespace if it doesn't exist.
    * @param name
    *   Namespace name
    * @return
    *   Future indicating success
    */
  def createNamespace(name: String)(implicit ec: ExecutionContext): Future[Unit]

  /** Deletes a namespace and all its resources.
    * @param name
    *   Namespace name
    * @return
    *   Future indicating success
    */
  def deleteNamespace(name: String)(implicit ec: ExecutionContext): Future[Unit]

  /** Retrieves a pod matching the given label selector.
    * @param labelSelector
    *   Kubernetes label selector (e.g., "app=spark,role=driver")
    * @param namespace
    *   Namespace to search in
    * @return
    *   Future containing the pod if found, None otherwise
    */
  def getPodByLabel(labelSelector: String, namespace: String)(implicit
      ec: ExecutionContext
  ): Future[Option[Pod]]

  /** Retrieves the ingress associated with a pod. Convention: Ingress name is "{podName}-ingress-1"
    * @param podName
    *   Name of the pod whose ingress to retrieve
    * @param namespace
    *   Namespace to search in
    * @return
    *   Future containing the ingress if found, None otherwise
    */
  def getIngressForPod(podName: String, namespace: String)(implicit
      ec: ExecutionContext
  ): Future[Option[Ingress]]

  /** Waits for a pod to reach Running state with ready condition. Polls every 2 seconds until
    * timeout.
    * @param labelSelector
    *   Label selector to identify the pod
    * @param namespace
    *   Namespace to search in
    * @param timeout
    *   Maximum time to wait
    * @return
    *   Future containing the running pod
    * @throws TimeoutException
    *   if pod doesn't reach running state within timeout
    */
  def waitForPodRunning(labelSelector: String, namespace: String, timeout: Duration)(implicit
      ec: ExecutionContext
  ): Future[Pod]
}

case class Pod(name: String, namespace: String, labels: Map[String, String], status: PodStatus)
case class PodStatus(phase: String, ready: Boolean)

case class Ingress(
    name: String,
    namespace: String,
    annotations: Map[String, String],
    rules: Seq[IngressRule]
)

case class IngressRule(host: Option[String], paths: Seq[IngressPath])
case class IngressPath(path: String, backend: IngressBackend)
case class IngressBackend(serviceName: String, servicePort: Int)

// TODO: Switch to using a proper Kubernetes client library (fabric8) in the future
/** Kubernetes client implementation using kubectl CLI.
  *
  *   - Uses kubectl commands executed via ProcessExecutor
  *   - Parses JSON output using Jackson without Scala collection converters (for 2.12/2.13
  *     compatibility)
  *   - Implements retry logic with exponential backoff for transient failures
  *   - All operations are async and non-blocking
  */
class K8sClient extends K8sOperations {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private val processTimeout = 5.seconds
  private val retryConfig    = RetryConfig(maxAttempts = 3, initialDelay = 500.millis)

  override def createNamespace(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      // Try to create namespace, ignore if already exists
      Try(
        ProcessExecutor.execute(
          Seq("kubectl", "create", "namespace", name),
          processTimeout
        )
      )
      ()
    }
  }

  override def deleteNamespace(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      ProcessExecutor.execute(
        Seq("kubectl", "delete", "namespace", name, "--wait=false"),
        processTimeout
      )
      ()
    }.recover { case _ =>
      () // Namespace might not exist
    }
  }

  override def getPodByLabel(
      labelSelector: String,
      namespace: String
  )(implicit ec: ExecutionContext): Future[Option[Pod]] = {
    Retry.withBackoff(retryConfig) {
      val result = ProcessExecutor.execute(
        Seq("kubectl", "get", "pods", "-n", namespace, "-l", labelSelector, "-o", "json"),
        processTimeout
      )
      parsePods(result.stdout).headOption
    }
  }

  override def getIngressForPod(
      podName: String,
      namespace: String
  )(implicit ec: ExecutionContext): Future[Option[Ingress]] = {
    Retry
      .withBackoff(retryConfig) {
        val ingressName = s"$podName-ingress-1"
        val result = ProcessExecutor.execute(
          Seq("kubectl", "get", "ingress", "-n", namespace, ingressName, "-o", "json"),
          processTimeout
        )
        parseIngress(result.stdout)
      }
      .recover { case _ =>
        None
      }
  }

  override def waitForPodRunning(
      labelSelector: String,
      namespace: String,
      timeout: Duration
  )(implicit ec: ExecutionContext): Future[Pod] = {
    val startTime     = System.currentTimeMillis()
    val timeoutMillis = timeout.toMillis

    def checkPod(): Future[Pod] = {
      getPodByLabel(labelSelector, namespace).flatMap {
        case Some(pod) if pod.status.phase == "Running" && pod.status.ready =>
          Future.successful(pod)
        case _ if (System.currentTimeMillis() - startTime) > timeoutMillis =>
          Future.failed(
            new TimeoutException(s"Pod with label $labelSelector not running within $timeout")
          )
        case _ =>
          val delay = Promise[Unit]()
          val timer = new java.util.Timer()
          timer.schedule(
            new java.util.TimerTask {
              def run(): Unit = {
                delay.success(())
                timer.cancel()
              }
            },
            2000
          )
          delay.future.flatMap(_ => checkPod())
      }
    }

    checkPod()
  }

  /** Parses kubectl JSON output into Pod objects. Uses manual iteration over Jackson nodes to avoid
    * Scala collection converter issues.
    */
  private def parsePods(json: String): Seq[Pod] = {
    Try {
      val root  = mapper.readTree(json)
      val items = root.path("items")

      val pods          = ArrayBuffer[Pod]()
      val itemsIterator = items.elements()
      while (itemsIterator.hasNext) {
        val item     = itemsIterator.next()
        val metadata = item.path("metadata")
        val status   = item.path("status")

        val labels = parseObjectToMap(metadata.path("labels"))

        // Check if pod has Ready=True condition
        val ready = {
          val conditions     = status.path("conditions")
          var isReady        = false
          val conditionsIter = conditions.elements()
          while (conditionsIter.hasNext && !isReady) {
            val condition = conditionsIter.next()
            if (
              condition.path("type").asText() == "Ready" &&
              condition.path("status").asText() == "True"
            ) {
              isReady = true
            }
          }
          isReady
        }

        pods += Pod(
          name = metadata.path("name").asText(),
          namespace = metadata.path("namespace").asText(),
          labels = labels,
          status = PodStatus(
            phase = status.path("phase").asText(),
            ready = ready
          )
        )
      }
      pods.toSeq
    }.getOrElse(Seq.empty)
  }

  private def parseIngress(json: String): Option[Ingress] = {
    Try {
      val root     = mapper.readTree(json)
      val metadata = root.path("metadata")
      val spec     = root.path("spec")

      val annotations = parseObjectToMap(metadata.path("annotations"))

      val rules     = ArrayBuffer[IngressRule]()
      val rulesIter = spec.path("rules").elements()
      while (rulesIter.hasNext) {
        val rule = rulesIter.next()
        val host = Option(rule.path("host").asText()).filter(_.nonEmpty)

        val paths     = ArrayBuffer[IngressPath]()
        val pathsIter = rule.path("http").path("paths").elements()
        while (pathsIter.hasNext) {
          val pathNode = pathsIter.next()
          val backend  = pathNode.path("backend")

          paths += IngressPath(
            path = pathNode.path("path").asText(),
            backend = IngressBackend(
              serviceName = backend.path("service").path("name").asText(),
              servicePort = backend.path("service").path("port").path("number").asInt()
            )
          )
        }

        rules += IngressRule(host, paths.toSeq)
      }

      Ingress(
        name = metadata.path("name").asText(),
        namespace = metadata.path("namespace").asText(),
        annotations = annotations,
        rules = rules.toSeq
      )
    }.toOption
  }

  /** Converts a Jackson JsonNode object to a Scala Map. Manual iteration avoids Scala version
    * compatibility issues with .asScala
    */
  private def parseObjectToMap(node: JsonNode): Map[String, String] = {
    val map        = scala.collection.mutable.Map[String, String]()
    val fieldsIter = node.fields()
    while (fieldsIter.hasNext) {
      val entry = fieldsIter.next()
      map += (entry.getKey -> entry.getValue.asText())
    }
    map.toMap
  }
}
