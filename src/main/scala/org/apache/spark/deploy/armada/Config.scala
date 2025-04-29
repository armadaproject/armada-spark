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
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

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
      .checkValue(urlPrefix => (urlPrefix.length > 0) && urlPrefix.startsWith("http", 0),
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
  private val labelSelectors: Regex = (s"^($label=$label(,$label=$label)*)?$$").r

  private[armada] val selectorsValidator: CharSequence => Boolean = selectors => {
    val selectorsMaybe = labelSelectors.findPrefixMatchOf(selectors)
    selectorsMaybe match {
      case Some(selectors) => true
      case None => false
    }
  }

  def transformSelectorsToMap(str: String): Map[String,String] = {
    if (str.trim.isEmpty) {
      Map()
    }
    else {
      str.split(",").map(a => a.split("=")(0) -> a.split("=")(1)).toMap
    }
  }

  val DEFAULT_CLUSTER_SELECTORS = ""

  val ARMADA_CLUSTER_SELECTORS: ConfigEntry[String] =
    ConfigBuilder("spark.armada.clusterSelectors")
      .doc("A comma separated list of kubernetes label selectors (in key=value format) to ensure " +
           "the spark driver and its executors are deployed to the same cluster.")
      .stringConf
      .checkValue(selectorsValidator, "Selectors must be valid kubernetes labels/selectors")
      .createWithDefaultString(DEFAULT_CLUSTER_SELECTORS)

  private[armada] val singleLabelOrNone: Regex = (s"^($label)?$$").r
  private[armada] val singleLabelValidator: CharSequence => Boolean = l => {
    val labelMaybe = singleLabelOrNone.findPrefixMatchOf(l)
    labelMaybe match {
      case Some(l) => true
      case None => false
    }
  }

  val GANG_SCHEDULING_NODE_UNIFORMITY_LABEL: ConfigEntry[String] =
    ConfigBuilder("spark.armada.scheduling.nodeUniformityLabel")
      .doc("A single kubernetes label to apply to both driver and executors")
      .stringConf
      .checkValue(singleLabelValidator, "Label must be a valid kubernetes label, just the label!")
      .createWithDefaultString("armada-spark")

  private val rfc1035labelPrefix: Regex = "([a-z][0-9a-z-]{0,29})?".r

  private[armada] val rfc1035labelPrefixValidator: CharSequence => Boolean = label => {
    val labelMaybe = rfc1035labelPrefix.findPrefixMatchOf(label)
    labelMaybe match {
      case Some(label) => true
      case None => false
    }
  }

  val DRIVER_SERVICE_NAME_PREFIX: ConfigEntry[String] =
    ConfigBuilder("spark.armada.driverServiceNamePrefix")
      .doc("Defines the driver's service name prefix within Armada. Lowercase a-z and '-' characters only. " +
        "Max length of 30 characters.")
      .stringConf
      .checkValue(rfc1035labelPrefixValidator, "Service name prefix must adhere to rfc 1035 label names.")
      .createWithDefaultString("armada-spark-driver-")
}
