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
package org.apache.spark.deploy.armada

import org.apache.spark.deploy.armada.validators.K8sValidator

import java.util.concurrent.TimeUnit
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, OptionalConfigEntry}

import scala.util.{Failure, Success, Try}

private[spark] object Config {
  private val invalidLabelListErrorMessage =
    "Must be a comma-separated list of valid Kubernetes labels (each as key=value)"

  val ARMADA_SERVER_INTERNAL_URL: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.internalUrl")
      .doc(
        "The Kubernetes DNS or IP address of the Armada Server. " +
          "This URL is used by the Driver when running in Cluster mode. " +
          "If not specified, 'spark.master' will be used."
      )
      .stringConf
      .createOptional

  val ARMADA_JOB_QUEUE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.queue")
      .doc(
        "The name of the job queue to use for the Armada job."
      )
      .stringConf
      .createOptional

  val ARMADA_JOB_SET_ID: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.jobSetId")
      .doc(
        "The JobSet ID for which the driver and executor pods will be part of. " +
          "If not set, it will be derived from the Spark application name."
      )
      .stringConf
      .createOptional

  val ARMADA_EXECUTOR_TRACKER_POLLING_INTERVAL: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.executor.trackerPollingInterval")
      .doc(
        "Interval between polls to check the " +
          "state of executors."
      )
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(
        _ > 0,
        s"Polling interval must be a" +
          " positive time value."
      )
      .createWithDefaultString("60s")

  val ARMADA_EXECUTOR_TRACKER_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.executor.trackerTimeout")
      .doc("Time to wait for the minimum number of executors.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(
        _ > 0,
        s"Timeout must be a" +
          " positive time value."
      )
      .createWithDefaultString("600s")

  val ARMADA_EXECUTOR_CONNECTION_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.executor.connectionTimeout")
      .doc("Time to wait for the executor to connect to the driver.")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(
        _ > 0,
        s"Connection timeout must be a positive time value."
      )
      .createWithDefaultString("300s")

  val ARMADA_LOOKOUTURL: ConfigEntry[String] =
    ConfigBuilder("spark.armada.lookouturl")
      .doc("URL base for the Armada Lookout UI.")
      .stringConf
      .checkValue(
        urlPrefix => urlPrefix.nonEmpty && urlPrefix.startsWith("http", 0),
        s"Value must be a valid URL, like http://host:8080 or https://host:443"
      )
      .createWithDefaultString("http://localhost:30000")

  val ARMADA_HEALTH_CHECK_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.health.checkTimeout")
      .doc("Number of seconds to wait for an Armada health check result")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ > 0, s"Timeout must be a positive time value.")
      .createWithDefaultString("5")

  val ARMADA_JOB_NODE_SELECTORS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.nodeSelectors")
      .doc(
        "A comma-separated list of kubernetes label selectors (in key=value format) to ensure " +
          "the spark driver and its executors are deployed to the same cluster."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.nodeUniformity")
      .doc(
        "Constrains the jobs that make up a gang to be scheduled across a uniform set of nodes. " +
          "Specifically, if set, all gang jobs are scheduled onto nodes for which the value of the provided label is equal."
      )
      .stringConf
      .checkValue(
        v => if (v.nonEmpty) K8sValidator.Label.isValidKey(v) else true,
        "Only a valid RFC 1123 label key is allowed!"
      )
      .createOptional

  val SPARK_DRIVER_SERVICE_NAME_PREFIX: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.serviceNamePrefix")
      .doc(
        "Prefix for the driver service name which should follow the RFC 1035 label names " +
          "specification. Should not be longer than 57 characters."
      )
      .stringConf
      .checkValue(
        v => v.length <= 57 && K8sValidator.Name.isValidPrefix(v),
        "Service name prefix must adhere to RFC 1035 label names."
      )
      .createWithDefaultString("armada-spark-driver-")

  val ARMADA_SPARK_POD_LABELS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.pod.labels")
      .doc(
        "A comma-separated list of kubernetes labels (in key=value format) to be added to " +
          "both the driver and executor pods."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val ARMADA_SPARK_DRIVER_LABELS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.labels")
      .doc(
        "A comma-separated list of kubernetes labels (in key=value format) to be added to the " +
          "driver pod."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val ARMADA_SPARK_JOB_NAMESPACE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.namespace")
      .doc(
        "The namespace to use for the job. If not set, the default namespace will be used."
      )
      .stringConf
      .createWithDefaultString("default")

  val ARMADA_SPARK_JOB_PRIORITY: ConfigEntry[Double] =
    ConfigBuilder("spark.armada.scheduling.priority")
      .doc(
        "The priority to use for the job. If not set, the default priority will be used."
      )
      .doubleConf
      .checkValue(
        _ >= 0,
        "Priority must be equal or greater than 0."
      )
      .createWithDefault(0)

  val ARMADA_SPARK_EXECUTOR_LABELS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.labels")
      .doc(
        "A comma-separated list of kubernetes labels (in key=value format) to be added to all " +
          "executor pods."
      )
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createOptional

  val CONTAINER_IMAGE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.container.image")
      .doc("Container image to use for Spark containers.")
      .stringConf
      .createOptional

  /** Converts a comma-separated list of key=value pairs into a Map.
    *
    * @param str
    *   The string to convert.
    * @return
    *   A Map of the key=value pairs.
    */
  def commaSeparatedLabelsToMap(str: String): Map[String, String] = {
    parseCommaSeparatedK8sLabels(str).map(_.get).toMap
  }

  private def k8sLabelListValidator(labelList: String): Boolean = {
    parseCommaSeparatedK8sLabels(labelList).forall(_.isSuccess)
  }

  private def parseCommaSeparatedK8sLabels(str: String): Seq[Try[(String, String)]] = {
    str.trim
      .split(",")
      .filter(_.nonEmpty)
      .map { entry =>
        entry.split("=", 2).map(_.trim) match {
          case Array(key, value)
              if K8sValidator.Label.isValidKey(key) && K8sValidator.Label.isValidValue(value) =>
            Success(key -> value)
          case _ =>
            Failure(
              new IllegalArgumentException(
                s"Invalid label format: '$entry' (expected key=value)"
              )
            )
        }
      }
  }
}
