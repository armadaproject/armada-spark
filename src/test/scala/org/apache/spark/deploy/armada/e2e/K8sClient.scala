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

import org.apache.spark.deploy.armada.Config
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.api.model.{NamespaceBuilder, Pod}
import io.fabric8.kubernetes.api.model.networking.v1.Ingress

import java.util.concurrent.TimeoutException
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/** Kubernetes client implementation using fabric8 Kubernetes client library. */
class K8sClient {
  private val client: KubernetesClient = new DefaultKubernetesClient()

  def createNamespace(name: String)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val namespace = new NamespaceBuilder()
      .withNewMetadata()
      .withName(name)
      .endMetadata()
      .build()
    client
      .namespaces()
      .createOrReplace(namespace)
    ()
  }

  def deleteNamespace(name: String)(implicit ec: ExecutionContext): Future[Unit] = Future {
    try {
      client
        .namespaces()
        .withName(name)
        .withGracePeriod(0)
        .delete()
    } catch {
      case _: Exception => // Ignore if it doesn't exist
    }
  }

  def getPodsByLabel(
      labelSelector: String,
      namespace: String
  )(implicit ec: ExecutionContext): Future[Seq[Pod]] = Future {
    getPods(labelSelector, namespace)
  }

  /** Get pod names and their node selectors for pods matching the given label selector in the
    * specified namespace.
    */
  def getPodsWithNodeSelectors(
      labelSelector: String,
      namespace: String
  )(implicit ec: ExecutionContext): Future[Seq[(String, Map[String, String])]] = Future {
    getPods(labelSelector, namespace).map { pod =>
      val podName       = pod.getMetadata.getName
      val nodeSelectors = pod.getSpec.getNodeSelector.asScala.toMap
      (podName, nodeSelectors)
    }
  }

  private def getPods(labelSelector: String, namespace: String): Seq[Pod] = {
    val labels = Config.commaSeparatedLabelsToMap(labelSelector)
    client
      .pods()
      .inNamespace(namespace)
      .withLabels(labels.asJava)
      .list()
      .getItems
      .asScala
      .toSeq
  }

  /** Armada jobs can be accompanied by an Ingress for accessing the underlying pod.
    *
    * This method retrieves the Ingress resource for a given pod name in the specified namespace.
    */
  def getIngressForPod(
      podName: String,
      namespace: String
  )(implicit ec: ExecutionContext): Future[Option[Ingress]] = Future {
    val ingressName = s"$podName-ingress-0"
    Option(
      client
        .network()
        .v1()
        .ingresses()
        .inNamespace(namespace)
        .withName(ingressName)
        .get()
    )
  }

  /** Waits for a pod with the specified label selector to reach the Running and Ready state within
    * the given timeout duration.
    */
  def waitForPodRunning(
      labelSelector: String,
      namespace: String,
      timeout: Duration
  )(implicit ec: ExecutionContext): Future[Pod] = {
    val startTime     = System.currentTimeMillis()
    val timeoutMillis = timeout.toMillis

    def checkPod(): Future[Pod] = {
      getPodsByLabel(labelSelector, namespace).flatMap { pods =>
        pods.headOption match {
          case Some(pod) if isPodRunningAndReady(pod) =>
            Future.successful(pod)
          case _ if (System.currentTimeMillis() - startTime) > timeoutMillis =>
            Future.failed(
              new TimeoutException(s"Pod with label $labelSelector not running within $timeout")
            )
          case _ =>
            Future {
              Thread.sleep(2000)
            }.flatMap(_ => checkPod())
        }
      }
    }

    checkPod()
  }

  def isPodRunningAndReady(pod: Pod): Boolean = {
    val status = pod.getStatus
    if (status == null) return false

    val phase      = status.getPhase
    val conditions = status.getConditions

    val ready = if (conditions != null) {
      conditions.asScala.exists { condition =>
        condition.getType == "Ready" && condition.getStatus == "True"
      }
    } else {
      false
    }

    phase == "Running" && ready
  }
}
