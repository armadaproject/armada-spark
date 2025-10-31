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
package org.apache.spark.scheduler.cluster.armada

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import api.submit.JobSubmitRequestItem
import io.armadaproject.armada.ArmadaClient
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.util.ThreadUtils

/**
 * Manages executor allocation via Armada job submission.
 *
 * Responsibilities:
 * - Periodically check executor demand vs supply
 * - Build PodSpecs for Spark executors
 * - Submit jobs to Armada queue
 * - Track pending/running executors
 * - Handle allocation failures
 */
private[spark] class ArmadaExecutorAllocator(
    armadaClient: ArmadaClient,
    queue: String,
    jobSetId: String,
    namespace: String,
    conf: SparkConf,
    applicationId: String,
    executorToJobId: ConcurrentHashMap[String, String],
    pendingExecutors: mutable.HashSet[String]) extends Logging {

  // Configuration
  private val batchSize = conf.get(ARMADA_ALLOCATION_BATCH_SIZE)
  private val maxPendingJobs = conf.get(ARMADA_MAX_PENDING_JOBS)
  private val checkInterval = conf.get(ARMADA_ALLOCATION_CHECK_INTERVAL)
  // State tracking
  private val totalExpectedExecutors = new ConcurrentHashMap[Int, Int]()
  private val rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]()
  private val executorIdCounter = new AtomicInteger(0)

  // Scheduler for periodic allocation
  private val allocationExecutor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("armada-allocator")

  // ========================================================================
  // LIFECYCLE
  // ========================================================================

  def start(): Unit = {
    logInfo(s"Starting Armada executor allocator: queue=$queue, jobSetId=$jobSetId")

    // Start periodic allocation loop
    allocationExecutor.scheduleWithFixedDelay(
      () => tryAllocateExecutors(),
      0,
      checkInterval,
      TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    logInfo("Stopping Armada executor allocator")
    ThreadUtils.shutdown(allocationExecutor)
  }

  // ========================================================================
  // ALLOCATION MANAGEMENT
  // ========================================================================

  def setTotalExpectedExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {
    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.synchronized {
        rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      }
      totalExpectedExecutors.put(rp.id, numExecs)
    }
    logDebug(s"Updated target executors: ${totalExpectedExecutors.asScala.toMap}")
  }

  /**
   * Main allocation loop - checks demand and submits jobs if needed.
   */
  private def tryAllocateExecutors(): Unit = {
    try {
      totalExpectedExecutors.asScala.foreach { case (rpId, target) =>
        val rp = rpIdToResourceProfile.synchronized {
          rpIdToResourceProfile.get(rpId)
        }

        rp match {
          case Some(resourceProfile) =>
            val pending = getPendingExecutorCount(rpId)
            val gap = target - pending

            if (gap > 0 && pending < maxPendingJobs) {
              val toAllocate = math.min(gap, batchSize)
              submitExecutorJobs(toAllocate, resourceProfile, rpId)
            }

          case None =>
            logWarning(s"No resource profile found for ID $rpId")
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error in allocation loop: ${e.getMessage}", e)
    }
  }

  /**
   * Submit executor jobs to Armada.
   */
  private def submitExecutorJobs(
      count: Int,
      resourceProfile: ResourceProfile,
      rpId: Int): Unit = {

    try {
      // Build minimal context for Armada submission using existing SparkConf
      val app = new org.apache.spark.deploy.armada.submit.ArmadaClientApplication()

      // Validate/load Armada job config from SparkConf
      val armadaJobConfig = app.validateArmadaJobConfig(conf)

      // Construct minimal ClientArguments; values are placeholders sufficient for executor submission path
      val clientArgs = org.apache.spark.deploy.armada.submit.ClientArguments(
        mainAppResource = org.apache.spark.deploy.k8s.submit.JavaMainAppResource(Some("local:///dev/null")),
        mainClass = "org.apache.spark.examples.SparkPi",
        driverArgs = Array.empty[String],
        proxyUser = (None: Option[String])
      )

      // Set the driverJobId from ARMADA_JOB_ID env var if present, otherwise fall back to applicationId
      val driverJobId = sys.env.getOrElse("ARMADA_JOB_ID", applicationId)

      val submittedJobIds = app.submitExecutorJobs(
        armadaClient,
        clientArgs,
        armadaJobConfig,
        conf,
        driverJobId,
        count
      )

      // Track pending executors and map executor IDs to Armada job IDs
      submittedJobIds.foreach { jobId =>
        val execId = s"${rpId}-${executorIdCounter.incrementAndGet()}"
        executorToJobId.put(execId, jobId)
        pendingExecutors.synchronized { pendingExecutors += execId }
        logInfo(s"Submitted executor (execId=$execId) as Armada jobId=$jobId for RP=$rpId")
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to submit $count executor jobs for RP=$rpId: ${e.getMessage}", e)
    }
  }

  // ========================================================================
  // HELPER METHODS
  // ========================================================================

  /**
   * Get priority for resource profile.
   */
  private def getPriority(resourceProfile: ResourceProfile): Double = {
    // Higher priority for default profile, lower for others
    if (resourceProfile.id == ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) {
      1.0
    } else {
      0.5
    }
  }

  /**
   * Count pending executors for a resource profile.
   */
  private def getPendingExecutorCount(rpId: Int): Int = {
    pendingExecutors.synchronized {
      // In a full implementation, would track RP per executor
      // For now, return total pending count
      pendingExecutors.size
    }
  }
}
