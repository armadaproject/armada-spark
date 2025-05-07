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

import java.util.concurrent.TimeUnit
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, DRIVER_CORES, OptionalConfigEntry}
import org.apache.spark.network.util.ByteUnit

import java.util.regex.Pattern
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

private[spark] object Config {
  val ARMADA_EXECUTOR_TRACKER_POLLING_INTERVAL: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.executor.trackerPollingInterval")
      .doc("Interval between polls to check the " +
        "state of executors.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"Polling interval must be a" +
        " positive time value.")
      .createWithDefaultString("60s")

  val ARMADA_EXECUTOR_TRACKER_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.executor.trackerTimeout")
      .doc("Time to wait for the minimum number of executors.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"Timeout must be a" +
        " positive time value.")
      .createWithDefaultString("600s")

  val ARMADA_LOOKOUTURL: ConfigEntry[String] =
    ConfigBuilder("spark.armada.lookouturl")
      .doc("URL base for the Armada Lookout UI.")
      .stringConf
      .checkValue(urlPrefix => urlPrefix.nonEmpty && urlPrefix.startsWith("http", 0),
        s"Value must be a valid URL, like http://host:8080 or https://host:443")
      .createWithDefaultString("http://localhost:30000")

  val ARMADA_HEALTH_CHECK_TIMEOUT: ConfigEntry[Long] =
    ConfigBuilder("spark.armada.health.checkTimeout")
      .doc("Number of seconds to wait for an Armada health check result")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(interval => interval > 0, s"Timeout must be a positive time value.")
      .createWithDefaultString("5")

  // See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
  // Clients do not use the prefix, therefore we just accept the name portion on label selector names.
  private val label = "([\\w&&[^-_.]]([\\w-_.]{0,61}[\\w&&[^-_.]])?)"
  private val labelSelectors: Regex = s"^($label=$label(,$label=$label)*)?$$".r

  private[armada] def selectorsValidator(selectors: CharSequence): Boolean = {
    labelSelectors.findPrefixMatchOf(selectors).isDefined
  }

  private def parseCommaSeparatedK8sLabels(str: String): Seq[Try[(String, String)]] = {
    str.trim.split(",").map(_.trim)
      .filter(_.nonEmpty)
      .map { entry =>
        entry.trim.split("=", 2).map(_.trim) match {
          case Array(key, value) if K8sLabelValidator.isValidKey(key) && K8sLabelValidator.isValidValue(value) =>
            Success(key -> value)
          case _ =>
            Failure(new IllegalArgumentException(
              s"Invalid selector format: '$entry' (expected key=value)"
            ))
        }
      }
  }

  /**
   * Converts a comma separated list of key=value pairs into a Map.
   *
   * @param str The string to convert.
   * @return A Map of the key=value pairs.
   */
  def commaSeparatedLabelsToMap(str: String): Map[String, String] = {
    parseCommaSeparatedK8sLabels(str).map(_.get).toMap
  }

  val DEFAULT_CLUSTER_SELECTORS = ""

  val ARMADA_CLUSTER_SELECTORS: ConfigEntry[String] =
    ConfigBuilder("spark.armada.clusterSelectors")
      .doc("A comma separated list of kubernetes label selectors (in key=value format) to ensure " +
           "the spark driver and its executors are deployed to the same cluster.")
      .stringConf
      .checkValue(selectorsValidator, "Selectors must be valid kubernetes labels/selectors")
      .createWithDefaultString(DEFAULT_CLUSTER_SELECTORS)

  private[armada] val singleLabelOrNone: Regex = s"^($label)?$$".r

  private[armada] def singleLabelValidator(l: CharSequence): Boolean = {
    singleLabelOrNone.findPrefixMatchOf(l).isDefined
  }

  val GANG_SCHEDULING_NODE_UNIFORMITY_LABEL: ConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.nodeUniformityLabel")
      .doc("A single kubernetes label to apply to both driver and executors")
      .stringConf
      .checkValue(singleLabelValidator, "Label must be a valid kubernetes label, just the label!")
      .createWithDefaultString("armada-spark")

  private val validServiceNamePrefix: Regex = "([a-z][0-9a-z-]{0,29})?".r

  private[armada] def serviceNamePrefixValidator(name: CharSequence): Boolean = {
    validServiceNamePrefix.findPrefixMatchOf(name).isDefined
  }

  val DRIVER_SERVICE_NAME_PREFIX: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driverServiceNamePrefix")
      .doc("Defines the driver's service name prefix within Armada. Lowercase a-z and '-' characters only. " +
        "Max length of 30 characters.")
      .stringConf
      .checkValue(serviceNamePrefixValidator, "Service name prefix must adhere to rfc 1035 label names.")
      .createWithDefaultString("armada-spark-driver-")

  private val invalidLabelListErrorMessage =
    "Must be a comma-separated list of valid Kubernetes labels (each as key=value)"

  val ARMADA_SPARK_GLOBAL_LABELS: ConfigEntry[String] =
    ConfigBuilder("spark.armada.global.labels")
      .doc("A comma separated list of kubernetes labels (in key=value format) to be added to all " +
        "both the driver and executor pods.")
      .stringConf
      .checkValue(selectorsValidator, invalidLabelListErrorMessage)
      .createWithDefaultString("")

  val ARMADA_SPARK_DRIVER_LABELS: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.labels")
      .doc("A comma separated list of kubernetes labels (in key=value format) to be added to the " +
        "driver pod.")
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createWithDefaultString("")

  val ARMADA_SPARK_EXECUTOR_LABELS: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.labels")
      .doc("A comma separated list of kubernetes labels (in key=value format) to be added to all " +
        "executor pods.")
      .stringConf
      .checkValue(k8sLabelListValidator, invalidLabelListErrorMessage)
      .createWithDefaultString("")

  private def k8sLabelListValidator(labelList: String): Boolean = {
    parseCommaSeparatedK8sLabels(labelList).forall(_.isSuccess)
  }

  val ARMADA_QUEUE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.queue")
      .doc("The Armada queue that will be used for running the driver and executor pods.")
      .version("1.0.0")
      .stringConf
      .createWithDefault("default")

  val DRIVER_JOB_SET_ID: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.job.set.id")
      .doc("The Armada job set id that will be used for running the driver pods.")
      .version("1.0.0")
      .stringConf
      .createWithDefault("Driver-Job-Set")

  val EXECUTOR_JOB_SET_ID: ConfigEntry[String] =
    ConfigBuilder("spark.armada.exutor.job.set.id")
      .doc("The Armada job set id that will be used for running the executor pods.")
      .version("1.0.0")
      .stringConf
      .createWithDefault("Executor-Job-Set")

  val ARMADA_NAMESPACE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.namespace")
      .doc("The namespace that will be used for running the driver and executor pods.")
      .version("1.0.0")
      .stringConf
      .createWithDefault("default")

  val ARMADA_REMOTE_MASTER: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.remote.master")
      .doc("The URL for the remote Armada master.")
      .version("1.0.0")
      .stringConf
      .createOptional

  val CONTAINER_IMAGE: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.container.image")
      .doc("Container image to use for Spark containers. Individual container types " +
        "(e.g. driver or executor) can also be configured to use different images if desired, " +
        "by setting the container type-specific image name.")
      .version("1.0.0")
      .stringConf
      .createOptional

  val DRIVER_CONTAINER_IMAGE: ConfigEntry[Option[String]] =
    ConfigBuilder("spark.armada.driver.container.image")
      .doc("Container image to use for the driver.")
      .version("1.0.0")
      .fallbackConf(CONTAINER_IMAGE)

  val EXECUTOR_CONTAINER_IMAGE: ConfigEntry[Option[String]] =
    ConfigBuilder("spark.armada.executor.container.image")
      .doc("Container image to use for the executors.")
      .version("1.0.0")
      .fallbackConf(CONTAINER_IMAGE)

  val CONTAINER_IMAGE_PULL_POLICY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.container.image.pullPolicy")
      .doc("Armada image pull policy. Valid values are Always, Never, and IfNotPresent.")
      .version("1.0.0")
      .stringConf
      .checkValues(Set("Always", "Never", "IfNotPresent"))
      .createWithDefault("IfNotPresent")

  val ARMADA_DRIVER_POD_NAME: OptionalConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.pod.name")
      .doc("Name of the driver pod.")
      .version("1.0.0")
      .stringConf
      .createOptional

  val DEFAULT_ARMADA_APP_ID = "armada-spark-app-id"
  val DEFAULT_SPARK_EXECUTOR_CORES = "1"
  val DEFAULT_CORES = "100m"
  val ARMADA_DRIVER_LIMIT_CORES: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.limit.cores")
      .doc("Specify the hard cpu limit for the driver pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_CORES)

  val ARMADA_DRIVER_REQUEST_CORES: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.request.cores")
      .doc("Specify the cpu request for the driver pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_CORES)


  val ARMADA_EXECUTOR_LIMIT_CORES: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.limit.cores")
      .doc("Specify the hard cpu limit for each executor pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_CORES)


  val ARMADA_EXECUTOR_REQUEST_CORES: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.request.cores")
      .doc("Specify the cpu request for each executor pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_CORES)


  val DEFAULT_MEM = "1Gi"
  val DEFAULT_SPARK_EXECUTOR_MEMORY = "1g"
  val ARMADA_DRIVER_LIMIT_MEMORY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.limit.memory")
      .doc("Specify the hard memory limit for the driver pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_MEM)

  val ARMADA_DRIVER_REQUEST_MEMORY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.request.memory")
      .doc("Specify the memory request for the driver pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_MEM)

  val ARMADA_EXECUTOR_LIMIT_MEMORY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.limit.memory")
      .doc("Specify the hard memory limit for each executor pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_MEM)

  val ARMADA_EXECUTOR_REQUEST_MEMORY: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.request.memory")
      .doc("Specify the memory request for each executor pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_MEM)

  val DEFAULT_STORAGE = "512Mi"
  val ARMADA_DRIVER_LIMIT_EPHEMERAL_STORAGE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.limit.ephemeral.storage")
      .doc("Specify the hard ephemeral storage limit for the driver pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_STORAGE)

  val ARMADA_DRIVER_REQUEST_EPHEMERAL_STORAGE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driver.request.ephemeral.storage")
      .doc("Specify the ephemeral storage request for the driver pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_STORAGE)

  val ARMADA_EXECUTOR_LIMIT_EPHEMERAL_STORAGE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.limit.ephemeral.storage")
      .doc("Specify the hard ephemeral storage limit for each executor pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_STORAGE)

  val ARMADA_EXECUTOR_REQUEST_EPHEMERAL_STORAGE: ConfigEntry[String] =
    ConfigBuilder("spark.armada.executor.request.ephemeral.storage")
      .doc("Specify the ephemeral storage request for each executor pod")
      .version("1.0.0")
      .stringConf
      .createWithDefaultString(DEFAULT_STORAGE)
}

object K8sLabelValidator {
  private val MaxLabelLength  = 63
  private val MaxPrefixLength = 253

  // DNS-1123 label: lowercase alphanumeric start/end, interior may have '-'
  private val DNS1123_LABEL     = "[a-z0-9]([a-z0-9\\-_/.]*[a-z0-9])?"
  // DNS-1123 subdomain: series of DNS1123_LABEL separated by '.', total ≤ 253 chars
  private val DNS1123_SUBDOMAIN = s"$DNS1123_LABEL(?:\\.$DNS1123_LABEL)*"

  // full key = optional prefix "/" name
  private val LABEL_KEY_REGEX   = s"^(?:$DNS1123_SUBDOMAIN/)?$DNS1123_LABEL$$"
  // value = empty or a DNS1123_LABEL
  private val LABEL_VALUE_REGEX = s"^$DNS1123_LABEL$$"

  private val keyPattern   = Pattern.compile(LABEL_KEY_REGEX)
  private val valuePattern = Pattern.compile(LABEL_VALUE_REGEX)

  /** true if `key` is a valid Kubernetes label key (optional DNS1123 subdomain prefix) */
  def isValidKey(key: String): Boolean = {
    if (key == null) return false

    // enforce per-segment length limits
    if (key.contains("/")) {
      val Array(prefix, name) = key.split("/", 2)
      if (prefix.length > MaxPrefixLength || name.length > MaxLabelLength) return false
    } else if (key.length > MaxLabelLength) {
      // un-prefixed keys are just a label
      return false
    }

    keyPattern.matcher(key).matches()
  }

  /** true if `value` is a valid Kubernetes label value (or empty) */
  def isValidValue(value: String): Boolean = {
    if (value == null) return false
    if (value.isEmpty) return true
    if (value.length > MaxLabelLength) return false

    valuePattern.matcher(value).matches()
  }
}