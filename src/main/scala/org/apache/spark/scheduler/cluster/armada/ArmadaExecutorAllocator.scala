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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import io.armadaproject.armada.ArmadaClient

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.deploy.armada.DeploymentModeHelper
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.util.ThreadUtils

/** Manages executor allocation via Armada job submission.
  *
  * Responsibilities:
  *   - Periodically check executor demand vs supply
  *   - Build PodSpecs for Spark executors
  *   - Submit jobs to Armada queue
  *   - Track pending/running executors
  *   - Handle allocation failures
  */
private[spark] class ArmadaExecutorAllocator(
    armadaClient: ArmadaClient,
    queue: String,
    jobSetId: String,
    conf: SparkConf,
    applicationId: String,
    backend: ArmadaClusterManagerBackend
) extends Logging {

  // Configuration
  private val batchSize      = conf.get(ARMADA_ALLOCATION_BATCH_SIZE)
  private val maxPendingJobs = conf.get(ARMADA_MAX_PENDING_JOBS)
  private val checkInterval  = conf.get(ARMADA_ALLOCATION_CHECK_INTERVAL)
  // State tracking
  private val totalExpectedExecutors = new ConcurrentHashMap[Int, Int]()
  private val rpIdToResourceProfile  = new mutable.HashMap[Int, ResourceProfile]()

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
      TimeUnit.MILLISECONDS
    )
  }

  def stop(): Unit = {
    logInfo("Stopping Armada executor allocator")
    ThreadUtils.shutdown(allocationExecutor)
  }

  // ========================================================================
  // ALLOCATION MANAGEMENT
  // ========================================================================

  def setTotalExpectedExecutors(resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {
    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.synchronized {
        rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      }
      totalExpectedExecutors.put(rp.id, numExecs)
    }
    logInfo(s"Updated target executors: ${totalExpectedExecutors.asScala.toMap}")
  }

  /** Main allocation loop - checks demand and submits jobs if needed.
    */
  private def tryAllocateExecutors(): Unit = {
    try {
      totalExpectedExecutors.asScala.foreach { case (rpId, target) =>
        val rp = rpIdToResourceProfile.synchronized {
          rpIdToResourceProfile.get(rpId)
        }

        rp match {
          case Some(resourceProfile) =>
            val currentCount = backend.getExecutorIds().size
            val pending      = backend.getPendingExecutorCount
            val gap          = target - (currentCount + pending)
            if (gap > 0 && pending < maxPendingJobs) {
              val toAllocate = math.min(gap, batchSize)
              submitExecutorJobs(toAllocate, rpId)
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

  /** Submit executor jobs to Armada.
    */
  private def submitExecutorJobs(count: Int, rpId: Int): Unit = {

    try {
      // Build minimal context for Armada submission using existing SparkConf
      val app = new org.apache.spark.deploy.armada.submit.ArmadaClientApplication()

      val modeHelper  = DeploymentModeHelper(conf)
      val driverJobId = sys.env.getOrElse("ARMADA_JOB_ID", applicationId)
      val driverHostname = if (modeHelper.isDriverInCluster) {
        None
      } else {
        Some(
          conf
            .getOption("spark.driver.host")
            .getOrElse(
              throw new IllegalArgumentException(
                "spark.driver.host must be set in client mode. " +
                  "Please set it via --conf spark.driver.host=<hostname> or ensure it's set in your Spark configuration."
              )
            )
        )
      }

      val submittedJobIds = app.validateAndSubmitExecutorJobs(
        armadaClient,
        conf,
        driverJobId,
        count,
        driverHostname
      )

      // Track pending executors and map executor IDs to Armada job IDs
      submittedJobIds.foreach { jobId =>
        val execId = backend.recordExecutor(jobId)
        backend.addPendingExecutor(execId)
        logInfo(s"Submitted executor (execId=$execId) as Armada jobId=$jobId for RP=$rpId")
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to submit $count executor jobs for RP=$rpId: ${e.getMessage}", e)
    }
  }
}
