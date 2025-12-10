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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.deploy.armada.submit.ArmadaUtils
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils

/** Helper trait that encapsulates deployment mode-specific behavior for Armada Spark jobs.
  *
  * This trait handles different combinations of deployment modes (client vs cluster) and executor
  * allocation strategies (static vs dynamic).
  */
trait DeploymentModeHelper {

  /** Returns the initial number of executors to allocate.
    *
    * For static allocation, this returns the configured executor count. For dynamic allocation,
    * this returns the minimum executor count.
    *
    * @return
    *   The number of executor pods to create
    */
  def getExecutorCount: Int

  /** Returns the gang scheduling cardinality.
    *
    * Gang scheduling ensures all pods in a group are scheduled together atomically. The cardinality
    * indicates how many pods must be scheduled as a unit.
    *
    * @return
    *   The total number of pods in the gang (executors + driver if applicable)
    */
  def getGangCardinality: Int

  /** Returns whether the driver runs inside the cluster (cluster mode) or externally (client mode).
    *
    * @return
    *   true if driver runs in cluster, false if driver runs externally
    */
  def isDriverInCluster: Boolean

  /** Returns whether executors should be proactively requested at startup.
    *
    * This is typically true for client mode with static allocation, where executors need to be
    * requested immediately since the driver is already running.
    *
    * @return
    *   true if executors should be proactively requested, false otherwise
    */
  def shouldProactivelyRequestExecutors: Boolean

  /** Returns the source for jobSetId based on deployment mode.
    *
    * In cluster mode, jobSetId comes from environment variable ARMADA_JOB_SET_ID. In client mode,
    * jobSetId comes from config or falls back to application ID.
    *
    * @param applicationId
    *   Spark application ID to use as fallback
    * @return
    *   Optional jobSetId string
    */
  def getJobSetIdSource(applicationId: String): Option[String]

  /** Returns the driver hostname based on deployment mode.
    *
    * In cluster mode, the driver runs in a pod and the hostname is derived from the service name
    * built from the driver job ID. In client mode, the driver runs externally and the hostname must
    * be provided via spark.driver.host configuration.
    *
    * @param driverJobId
    *   The Armada job ID of the driver pod
    * @return
    *   The driver hostname string
    * @throws IllegalArgumentException
    *   In client mode if spark.driver.host is not configured
    */
  def getDriverHostName(driverJobId: String): String
}

/** Static allocation in cluster mode.
  *
  * In this mode:
  *   - A fixed number of executors is allocated upfront
  *   - The driver runs as a pod inside the cluster
  *   - Gang cardinality includes both driver and executors
  */
class StaticCluster(val conf: SparkConf) extends DeploymentModeHelper {
  override def getExecutorCount: Int = {
    SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
  }

  override def getGangCardinality: Int = {
    // In cluster mode, include the driver pod in gang scheduling
    getExecutorCount + 1
  }

  override def isDriverInCluster: Boolean = true

  override def shouldProactivelyRequestExecutors: Boolean = false

  override def getJobSetIdSource(applicationId: String): Option[String] = {
    sys.env.get("ARMADA_JOB_SET_ID")
  }

  override def getDriverHostName(driverJobId: String): String = {
    // In cluster mode, driver runs in a pod, so use service name from job ID
    ArmadaUtils.buildServiceNameFromJobId(driverJobId)
  }
}

/** Static allocation in client mode.
  *
  * In this mode:
  *   - A fixed number of executors is allocated upfront
  *   - The driver runs on the client machine (outside the cluster)
  *   - Gang cardinality includes only executors
  */
class StaticClient(val conf: SparkConf) extends DeploymentModeHelper {
  override def getExecutorCount: Int = {
    SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
  }

  override def getGangCardinality: Int = {
    // In client mode, driver runs externally, so only count executors
    getExecutorCount
  }

  override def isDriverInCluster: Boolean = false

  override def shouldProactivelyRequestExecutors: Boolean = true

  override def getJobSetIdSource(applicationId: String): Option[String] = {
    conf.get(ARMADA_JOB_SET_ID).orElse(Some(applicationId))
  }

  override def getDriverHostName(driverJobId: String): String = {
    // In client mode, driver runs externally, so use spark.driver.host from config
    conf
      .getOption("spark.driver.host")
      .getOrElse(
        throw new IllegalArgumentException(
          "spark.driver.host must be set in client mode. " +
            "Please set it via --conf spark.driver.host=<hostname> or ensure it's set in your Spark configuration."
        )
      )
  }
}

/** Dynamic allocation in cluster mode.
  *
  * In this mode:
  *   - Executors are allocated/deallocated dynamically based on workload
  *   - The driver runs as a pod inside the cluster
  *   - Initial allocation uses the configured minimum executor count
  *   - Gang cardinality includes both driver and minimum executors
  */
class DynamicCluster(val conf: SparkConf) extends DeploymentModeHelper {
  override def getExecutorCount: Int = {
    // For dynamic allocation, use minExecutors as the initial count
    conf.getInt(
      "spark.dynamicAllocation.minExecutors",
      SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    )
  }

  override def getGangCardinality: Int = {
    // In cluster mode, include the driver pod in gang scheduling
    getExecutorCount + 1
  }

  override def isDriverInCluster: Boolean = true

  override def shouldProactivelyRequestExecutors: Boolean = false

  override def getJobSetIdSource(applicationId: String): Option[String] = {
    sys.env.get("ARMADA_JOB_SET_ID")
  }

  override def getDriverHostName(driverJobId: String): String = {
    // In cluster mode, driver runs in a pod, so use service name from job ID
    ArmadaUtils.buildServiceNameFromJobId(driverJobId)
  }
}

/** Dynamic allocation in client mode.
  *
  * In this mode:
  *   - Executors are allocated/deallocated dynamically based on workload
  *   - The driver runs on the client machine (outside the cluster)
  *   - Initial allocation uses the configured minimum executor count
  *   - Gang cardinality includes only minimum executors
  */
class DynamicClient(val conf: SparkConf) extends DeploymentModeHelper {
  override def getExecutorCount: Int = {
    // For dynamic allocation, use minExecutors as the initial count
    conf.getInt(
      "spark.dynamicAllocation.minExecutors",
      SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    )
  }

  override def getGangCardinality: Int = {
    // In client mode, driver runs externally, so only count executors
    getExecutorCount
  }

  override def isDriverInCluster: Boolean = false

  override def shouldProactivelyRequestExecutors: Boolean = false

  override def getJobSetIdSource(applicationId: String): Option[String] = {
    conf.get(ARMADA_JOB_SET_ID).orElse(Some(applicationId))
  }

  override def getDriverHostName(driverJobId: String): String = {
    // In client mode, driver runs externally, so use spark.driver.host from config
    conf
      .getOption("spark.driver.host")
      .getOrElse(
        throw new IllegalArgumentException(
          "spark.driver.host must be set in client mode. " +
            "Please set it via --conf spark.driver.host=<hostname> or ensure it's set in your Spark configuration."
        )
      )
  }
}

/** Factory for creating ModeHelper instances based on Spark configuration.
  */
object DeploymentModeHelper {

  /** Creates the appropriate ModeHelper implementation based on deployment configuration.
    *
    * @return
    *   A ModeHelper instance appropriate for the configured deployment mode
    */
  def apply(conf: SparkConf): DeploymentModeHelper = {
    val deployMode = conf.get("spark.submit.deployMode", "client")
    val isDynamic  = conf.getBoolean("spark.dynamicAllocation.enabled", false)

    (deployMode.toLowerCase, isDynamic) match {
      case ("cluster", true)  => new DynamicCluster(conf)
      case ("cluster", false) => new StaticCluster(conf)
      case ("client", true)   => new DynamicClient(conf)
      case _                  => new StaticClient(conf)
    }
  }
}
