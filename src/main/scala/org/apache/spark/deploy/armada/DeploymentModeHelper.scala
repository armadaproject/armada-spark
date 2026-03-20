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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.deploy.armada.submit.ArmadaUtils
import org.apache.spark.internal.Logging
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
    * Returns the config-derived gang size unless gang node affinity has already been captured, in
    * which case returns 0 (no gang constraint needed for subsequent allocations).
    *
    * @return
    *   The total number of pods in the gang, or 0 if gang affinity already established
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

  /** Captures gang node-uniformity attributes from the first executor's environment.
    *
    * In dynamic client mode with nodeUniformity configured, this stores the label name/value from
    * the initial gang into SparkConf so that subsequent allocations can use them. Only captures
    * once; subsequent calls are no-ops.
    *
    * @param attributes
    *   Map of executor environment attributes
    */
  def captureGangAttributes(attributes: Map[String, String]): Unit

  /** Returns whether the allocator should submit more executor requests.
    *
    * In dynamic client mode with nodeUniformity, this returns false until gang attributes have been
    * captured from the initial executor batch.
    *
    * @return
    *   true if additional allocations are allowed
    */
  def isReadyToAllocateMore: Boolean

  /** Returns the gang node selector derived from captured attributes.
    *
    * @return
    *   Map of label name to label value, or empty if not yet captured
    */
  def getGangNodeSelector: Map[String, String]

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

  override def getGangCardinality: Int = getExecutorCount + 1

  override def isDriverInCluster: Boolean = true

  override def shouldProactivelyRequestExecutors: Boolean = false

  override def getJobSetIdSource(applicationId: String): Option[String] = {
    sys.env.get("ARMADA_JOB_SET_ID")
  }

  override def getDriverHostName(driverJobId: String): String = {
    // In cluster mode, driver runs in a pod, so use service name from job ID
    ArmadaUtils.buildServiceNameFromJobId(driverJobId)
  }

  override def captureGangAttributes(attributes: Map[String, String]): Unit = {}
  override def isReadyToAllocateMore: Boolean                               = true
  override def getGangNodeSelector: Map[String, String]                     = Map.empty

  override def toString: String = "StaticCluster"
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

  override def captureGangAttributes(attributes: Map[String, String]): Unit = {}
  override def isReadyToAllocateMore: Boolean                               = true
  override def getGangNodeSelector: Map[String, String]                     = Map.empty

  override def toString: String = "StaticClient"
}

/** Base class for dynamic allocation modes, holding shared gang lifecycle logic.
  *
  * Provides captureGangAttributes and getGangNodeSelector. Subclasses must implement
  * isReadyToAllocateMore and all other DeploymentModeHelper methods.
  */
private[armada] abstract class DynamicModeHelper(conf: SparkConf)
    extends DeploymentModeHelper
    with Logging {

  protected val gangAttributesCaptured = new AtomicBoolean(false)
  protected val nodeUniformityConfigured: Boolean =
    conf.get(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY).exists(_.nonEmpty)

  override def captureGangAttributes(attributes: Map[String, String]): Unit = {
    if (!nodeUniformityConfigured || gangAttributesCaptured.get()) return
    val labelName =
      attributes.getOrElse("ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME", "")
    val labelValue =
      attributes.getOrElse("ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE", "")
    if (labelName.nonEmpty && labelValue.nonEmpty) {
      conf.set(ARMADA_INTERNAL_GANG_NODE_LABEL_NAME.key, labelName)
      conf.set(ARMADA_INTERNAL_GANG_NODE_LABEL_VALUE.key, labelValue)
      gangAttributesCaptured.compareAndSet(false, true)
      logInfo(s"Captured gang node selector: $labelName=$labelValue")
    }
  }

  override def getGangNodeSelector: Map[String, String] = {
    val name  = conf.get(ARMADA_INTERNAL_GANG_NODE_LABEL_NAME).getOrElse("")
    val value = conf.get(ARMADA_INTERNAL_GANG_NODE_LABEL_VALUE).getOrElse("")
    if (name.nonEmpty && value.nonEmpty) Map(name -> value) else Map.empty
  }
}

/** Dynamic allocation in cluster mode.
  *
  * In this mode:
  *   - Executors are allocated/deallocated dynamically based on workload
  *   - The driver runs as a pod inside the cluster
  *   - Gang cardinality uses initialExecutors + 1 (driver) for one-time cluster discovery
  *   - submitArmadaJob uses getExecutorCount (= initialExecutors) for the initial batch
  */
class DynamicCluster(conf: SparkConf) extends DynamicModeHelper(conf) {

  // The initial executor count used for the one-time gang bootstrap.
  // submitArmadaJob reads getExecutorCount to know how many executors to
  // submit with the driver, so getExecutorCount returns this value.
  private val initialExecutorCount: Int =
    SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  // Armada is multi-cluster: nodeUniformity must be configured so Armada knows
  // which label to use for co-locating the gang and for subsequent scale-up node selectors.
  if (!nodeUniformityConfigured) {
    throw new IllegalArgumentException(
      "spark.armada.scheduling.nodeUniformity must be configured in " +
        "dynamic cluster mode. Armada is multi-cluster and requires " +
        "a node uniformity label to co-locate all executors on the " +
        "same cluster."
    )
  }

  // In cluster mode, driver is part of the gang, so 1 executor + driver = cardinality 2.
  if (initialExecutorCount < 1) {
    throw new IllegalArgumentException(
      s"spark.dynamicAllocation.initialExecutors must be >= 1 in " +
        s"dynamic cluster mode, but got: $initialExecutorCount. " +
        s"Armada requires gang cardinality >= 2 (driver + executors) " +
        s"to co-locate pods in the same cluster."
    )
  }

  // Return initialExecutors for the submission batch count.
  // submitArmadaJob reads this to decide how many executors to submit with the driver.
  override def getExecutorCount: Int = initialExecutorCount

  // Before capture: initialExecutorCount + 1 (driver included in gang).
  // After capture: return 0 (subsequent submissions use node selector instead).
  override def getGangCardinality: Int =
    if (getGangNodeSelector.nonEmpty) 0 else initialExecutorCount + 1

  override def isDriverInCluster: Boolean = true

  override def shouldProactivelyRequestExecutors: Boolean = false

  override def getJobSetIdSource(applicationId: String): Option[String] = {
    sys.env.get("ARMADA_JOB_SET_ID")
  }

  override def getDriverHostName(driverJobId: String): String = {
    // In cluster mode, driver runs in a pod, so use service name from job ID
    ArmadaUtils.buildServiceNameFromJobId(driverJobId)
  }

  // Driver captures gang attributes from its own env vars synchronously
  // at backend.start(), before the allocator runs. No gating needed.
  override def isReadyToAllocateMore: Boolean = true

  override def toString: String = "DynamicCluster"
}

/** Dynamic allocation in client mode.
  *
  * In this mode:
  *   - Executors are allocated/deallocated dynamically based on workload
  *   - The driver runs on the client machine (outside the cluster)
  *   - Gang cardinality uses initialExecutors for the one-time cluster discovery
  *   - minExecutors is purely a scale-down floor (can be 0)
  */
class DynamicClient(conf: SparkConf) extends DynamicModeHelper(conf) {

  // The initial executor count used for the one-time gang bootstrap.
  // This is the batch Spark's ExecutorAllocationManager requests at startup.
  // It is independent of minExecutors (the scale-down floor).
  private val initialExecutorCount: Int =
    SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  // Armada is multi-cluster: We use gang scheduling to co-locate the initial batch on one cluster,
  // then capture the cluster label for all subsequent submissions.
  // nodeUniformity must be configured so Armada knows which label to use.
  if (!nodeUniformityConfigured) {
    throw new IllegalArgumentException(
      "spark.armada.scheduling.nodeUniformity must be configured in " +
        "dynamic client mode. Armada is multi-cluster and requires " +
        "a node uniformity label to co-locate all executors on the " +
        "same cluster."
    )
  }

  // Gang cardinality for the initial batch must be >= 2 for Armada to
  // inject the gang env vars needed for cluster discovery.
  if (initialExecutorCount < 2) {
    throw new IllegalArgumentException(
      s"spark.dynamicAllocation.initialExecutors must be >= 2 in " +
        s"dynamic client mode, but got: $initialExecutorCount. " +
        s"Armada requires gang cardinality >= 2 to co-locate " +
        s"executors in the same cluster."
    )
  }

  override def getExecutorCount: Int = {
    // minExecutors is the scale-down floor. Can be 0.
    conf.getInt(
      "spark.dynamicAllocation.minExecutors",
      SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    )
  }

  // Before capture: use initialExecutorCount for gang cardinality (the bootstrap batch).
  // After capture: return 0 (subsequent submissions use node selector instead).
  override def getGangCardinality: Int =
    if (getGangNodeSelector.nonEmpty) 0 else initialExecutorCount

  override def isDriverInCluster: Boolean = false

  override def shouldProactivelyRequestExecutors: Boolean = false

  override def getJobSetIdSource(applicationId: String): Option[String] = {
    conf.get(ARMADA_JOB_SET_ID).orElse(Some(applicationId))
  }

  override def getDriverHostName(driverJobId: String): String = {
    conf
      .getOption("spark.driver.host")
      .getOrElse(
        throw new IllegalArgumentException(
          "spark.driver.host must be set in client mode. " +
            "Please set it via --conf spark.driver.host=<hostname> or ensure it's set in your Spark configuration."
        )
      )
  }

  // Always gate on gang attribute capture. nodeUniformity is required (validated above).
  override def isReadyToAllocateMore: Boolean = {
    val ready = gangAttributesCaptured.get()
    if (!ready) {
      logDebug(
        "Waiting for gang attributes from initial executor " +
          "before allocating more"
      )
    }
    ready
  }

  override def toString: String = "DynamicClient"
}

/** Factory for creating ModeHelper instances based on Spark configuration.
  */
object DeploymentModeHelper {

  /** Creates the appropriate ModeHelper implementation based on deployment configuration.
    *
    * Detects cluster mode via config or the ARMADA_JOB_SET_ID env var. The env var check is needed
    * because Spark rewrites deployMode to "client" inside cluster-mode driver pods.
    *
    * @return
    *   A ModeHelper instance appropriate for the configured deployment mode
    */
  def apply(conf: SparkConf): DeploymentModeHelper = {
    val deployMode = conf.get("spark.submit.deployMode", "cluster")
    val isCluster =
      deployMode.equalsIgnoreCase("cluster") ||
        sys.env.contains("ARMADA_JOB_SET_ID")
    val isDynamic = conf.getBoolean("spark.dynamicAllocation.enabled", false)

    (isCluster, isDynamic) match {
      case (true, true)   => new DynamicCluster(conf)
      case (true, false)  => new StaticCluster(conf)
      case (false, true)  => new DynamicClient(conf)
      case (false, false) => new StaticClient(conf)
    }
  }
}
