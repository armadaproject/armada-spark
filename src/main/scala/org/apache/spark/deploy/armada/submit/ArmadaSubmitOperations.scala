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
package org.apache.spark.deploy.armada.submit

// import scala.jdk.CollectionConverters._

/*
import K8SSparkSubmitOperation.getGracePeriod
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
*/

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmitOperation
// import org.apache.spark.deploy.armada.Config.{ARMADA_SUBMIT_GRACE_PERIOD}
// import org.apache.spark.deploy.k8s.Constants.{SPARK_POD_DRIVER_ROLE, SPARK_ROLE_LABEL}
// import org.apache.spark.deploy.k8s.KubernetesUtils.formatPodState
import org.apache.spark.util.{CommandLineLoggingUtils/* , Utils */}

private sealed trait ArmadaSubmitOperation extends CommandLineLoggingUtils {
  // TODO: Tear this out
}

private class KillApplication extends ArmadaSubmitOperation  {
  // TODO: Fill in.
}

private class ListStatus extends ArmadaSubmitOperation {
  // TODO: Fill in.
}

private[spark] class ArmadaSparkSubmitOperation extends SparkSubmitOperation
  with CommandLineLoggingUtils {

  private def isGlob(name: String): Boolean = {
    name.last == '*'
  }

  def execute(submissionId: String, sparkConf: SparkConf, op: ArmadaSubmitOperation): Unit = {
    /*
    val master = KubernetesUtils.parseMasterUrl(sparkConf.get("spark.master"))
    submissionId.split(":", 2) match {
      case Array(part1, part2@_*) =>
        val namespace = if (part2.isEmpty) None else Some(part1)
        val pName = if (part2.isEmpty) part1 else part2.headOption.get
        Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
          master,
          namespace,
          KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
          SparkKubernetesClientFactory.ClientType.Submission,
          sparkConf,
          None)
        ) { kubernetesClient =>
          implicit val client: KubernetesClient = kubernetesClient
          if (isGlob(pName)) {
            val ops = namespace match {
              case Some(ns) =>
                kubernetesClient
                  .pods
                  .inNamespace(ns)
              case None =>
                kubernetesClient
                  .pods
            }
            val pods = ops
              .withLabel(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
              .list()
              .getItems
              .asScala
              .filter { pod =>
                pod.getMetadata.getName.startsWith(pName.stripSuffix("*"))
              }.toList
            op.executeOnGlob(pods, namespace, sparkConf)
          } else {
            op.executeOnPod(pName, namespace, sparkConf)
          }
        }
      case _ =>
        printErrorAndExit(s"Submission ID: {$submissionId} is invalid.")
    }
    */
    printMessage("TODO!! ArmadaSparkSubmitOperation:execute!")
    ()
  }

  override def kill(submissionId: String, conf: SparkConf): Unit = {
    printMessage(s"TODO!! IMPLEMENT!! Armada: Submitting a request to kill submission " +
      s"${submissionId} in ${conf.get("spark.master")}. ")
    execute(submissionId, conf, new KillApplication)
  }

  override def printSubmissionStatus(submissionId: String, conf: SparkConf): Unit = {
    printMessage(s"TODO!! IMPLEMENT!! Armada: Submitting a request for the status of submission" +
      s" ${submissionId} in ${conf.get("spark.master")}.")
    execute(submissionId, conf, new ListStatus)
  }

  override def supports(master: String): Boolean = {
    master.startsWith("armada://")
  }
}
