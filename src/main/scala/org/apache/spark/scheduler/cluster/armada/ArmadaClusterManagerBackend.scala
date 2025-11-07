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

import scala.collection.mutable
import scala.concurrent.Future

import api.submit.JobCancelRequest
import io.armadaproject.armada.ArmadaClient
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.deploy.armada.submit.ArmadaUtils
import org.apache.spark.internal.config.SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}
import org.apache.spark.scheduler.{ExecutorDecommission, ExecutorDecommissionInfo, ExecutorExited, ExecutorKilled, ExecutorLossReason, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.k8s.GenerateExecID
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * Spark scheduler backend implementation for Armada.
 *
 * This backend manages executor lifecycle by submitting jobs to Armada queues.
 */
private[spark] class ArmadaClusterManagerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    executorService: ScheduledExecutorService,
    masterURL: String
) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  // ========================================================================
  // STATE MANAGEMENT
  // ========================================================================

  /** Maps Spark executor ID to Armada job ID */
  private val executorToJobId = new ConcurrentHashMap[String, String]()

  /** Maps Armada job ID to Spark executor ID */
  private val jobIdToExecutor = new ConcurrentHashMap[String, String]()

  /** Tracks requested executors not yet seen in events */
  private val pendingExecutors = new mutable.HashSet[String]()

  /** Default resource profile */
  private val defaultProfile = scheduler.sc.resourceProfileManager.defaultResourceProfile

  /** Initial executor count */
  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  /** Minimum registered ratio for starting */
  protected override val minRegisteredRatio =
    if (conf.get(SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  /** Armada connection details */
  private var grpcChannel: Option[ManagedChannel] = None
  private var armadaClient: Option[ArmadaClient] = None
  private var eventWatcher: Option[ArmadaEventWatcher] = None
  private var queue: Option[String] = None
  private var jobSetId: Option[String] = None
  private var namespace: Option[String] = None

  /** Executor allocation manager */
  private var executorAllocator: Option[ArmadaExecutorAllocator] = None


  override def applicationId(): String = {
    conf.getAppId
  }

  // ========================================================================
  // LIFECYCLE METHODS
  // ========================================================================

  override def start(): Unit = {
    super.start()

    // Initialize Armada event watcher if queue and jobSetId are provided
    val queueOpt    = conf.get(ARMADA_JOB_QUEUE)
    val jobSetIdOpt = sys.env.get("ARMADA_JOB_SET_ID")



    (queueOpt, jobSetIdOpt) match {
      case (Some(q), Some(jsId)) =>
        try {
          queue = Some(q)
          jobSetId = Some(jsId)
          namespace = conf.get(ARMADA_SPARK_JOB_NAMESPACE)

          val serverUrl = conf.get(ARMADA_SERVER_INTERNAL_URL).getOrElse(masterURL)
          val (host, port) = ArmadaUtils.parseMasterUrl(serverUrl)

          logInfo(s"Starting Armada cluster scheduler backend: " +
            s"jobSetId=$jsId, queue=$q, namespace=$namespace")

          // Build gRPC channel to Armada server
          val channel: ManagedChannel = ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
            .build()
          grpcChannel = Some(channel)

          // Initialize Armada client
          val token = conf.get(ARMADA_AUTH_TOKEN)
          val client = ArmadaClient(host, port, useSsl = false, token)
          armadaClient = Some(client)

          // Start event watcher
          val watcher = new ArmadaEventWatcher(channel, q, jsId, this, jobIdToExecutor, client)
          eventWatcher = Some(watcher)
          watcher.start()
          logInfo(s"Armada Event Watcher started for queue=$q jobSetId=$jsId at $host:$port")

          // Initialize executor allocator if needed
          namespace.foreach { ns =>
            val allocator = new ArmadaExecutorAllocator(
              client,
              q,
              jsId,
              ns,
              conf,
              applicationId(),
              executorToJobId,
              jobIdToExecutor,
              pendingExecutors)
            executorAllocator = Some(allocator)
            allocator.start()
          }

          // Submit initial executors if allocator is available
          executorAllocator.foreach { allocator =>
            val initExecs = Map(defaultProfile -> initialExecutors)
            allocator.setTotalExpectedExecutors(initExecs)
          }

        } catch {
          case e: Throwable =>
            logWarning(s"Failed to start Armada components: ${e.getMessage}", e)
        }

      case _ =>
        logInfo("Armada queue or jobSetId not configured; backend started in passive mode")
    }
  }

  override def stop(): Unit = {
    logInfo("Stopping Armada cluster scheduler backend")


    // Call parent stop
    Utils.tryLogNonFatalError {
      super.stop()
    }

    // Stop event watcher
    Utils.tryLogNonFatalError {
      eventWatcher.foreach(_.stop())
    }

    // Stop executor allocator
    Utils.tryLogNonFatalError {
      executorAllocator.foreach(_.stop())
    }

    // Cancel all jobs in the job set if configured
    if (conf.get(ARMADA_DELETE_EXECUTORS)) {
      Utils.tryLogNonFatalError {
        (armadaClient, jobSetId) match {
          case (Some(client), Some(jsId)) =>
            client.cancelJobSet(jsId)
            logInfo(s"Cancelled Armada job set: $jsId")
          case _ =>
        }
      }
    }

    // Shutdown executor service
    Utils.tryLogNonFatalError {
      ThreadUtils.shutdown(executorService)
    }

    // Shutdown gRPC channel
    Utils.tryLogNonFatalError {
      grpcChannel.foreach { ch =>
        ch.shutdown()
        ch.awaitTermination(5, TimeUnit.SECONDS)
      }
    }

    grpcChannel = None
    eventWatcher = None
    armadaClient = None
    executorAllocator = None
  }

  // ========================================================================
  // EXECUTOR ALLOCATION
  // ========================================================================

  override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    executorAllocator match {
      case Some(allocator) =>
        allocator.setTotalExpectedExecutors(resourceProfileToTotalExecs)
        Future.successful(true)
      case None =>
        logWarning("Executor allocator not initialized; cannot request executors")
        Future.successful(false)
    }
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def getExecutorIds(): Seq[String] = synchronized {
    super.getExecutorIds()
  }

  // ========================================================================
  // EXECUTOR TERMINATION
  // ========================================================================

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    logInfo(s"Killing ${executorIds.size} executors: ${executorIds.mkString(", ")}")

    // Send RPC kill signal to executors
    executorIds.foreach { id =>
      removeExecutor(id, ExecutorKilled)
    }

    // Cancel Armada jobs after grace period
    val killTask = new Runnable() {
      override def run(): Unit = Utils.tryLogNonFatalError {
        val jobIds = executorIds.flatMap(id =>
          Option(executorToJobId.get(id)))

        if (jobIds.nonEmpty) {
          (armadaClient, queue, jobSetId) match {
            case (Some(client), Some(q), Some(jsId)) =>
              val cancelRequest = JobCancelRequest(
                jobSetId = jsId,
                jobId = "",
                jobIds = jobIds,
                queue = q,
                reason = "Executor killed by Spark")

              client.cancelJobs(cancelRequest)
              logInfo(s"Cancelled ${jobIds.size} Armada jobs")
            case _ =>
              logWarning("Cannot cancel jobs: Armada client not initialized")
          }
        }
      }
    }

    executorService.schedule(
      killTask,
      conf.get(ARMADA_KILL_GRACE_PERIOD),
      TimeUnit.MILLISECONDS)

    Future.successful(true)
  }

  // ========================================================================
  // DECOMMISSIONING SUPPORT
  // ========================================================================

  override def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean): Seq[String] = {

    val executorIds = executorsAndDecomInfo.map(_._1)
    logInfo(s"Decommissioning ${executorIds.length} executors: ${executorIds.mkString(", ")}")

    // Delegate to parent - it handles everything via BlockManagerDecommissioner
    super.decommissionExecutors(
      executorsAndDecomInfo,
      adjustTargetNumExecutors,
      triggeredByExecutor)
  }
  
  // ========================================================================
  // ARMADA-SPECIFIC EVENT HANDLERS
  // ========================================================================

  /**
   * Called by event watcher when executor job is running
   */
  private[armada] def onExecutorRunning(jobId: String, executorId: String): Unit = {
    pendingExecutors.synchronized {
      pendingExecutors -= executorId
    }
    logInfo(s"Executor $executorId (job $jobId) is running")
  }

  /**
   * Called by event watcher when executor job fails
   */
  private[armada] def onExecutorFailed(
      jobId: String,
      executorId: String,
      exitCode: Int,
      reason: String): Unit = {

    val exitReason = ExecutorExited(
      exitCode,
      exitCausedByApp = exitCode != 0,
      s"Armada job $jobId failed: $reason")

    removeExecutor(executorId, exitReason)
  }

  /**
   * Called by event watcher when executor job is cancelled
   */
  private[armada] def onExecutorCancelled(jobId: String, executorId: String): Unit = {
    val exitReason = ExecutorExited(
      -1,
      exitCausedByApp = false,
      s"Armada job $jobId was cancelled")

    removeExecutor(executorId, exitReason)
  }

  /**
   * Called when Armada signals a job is being preempted.
   * Proactively start decommissioning.
   */
  private[armada] def onArmadaPreempting(jobId: String): Unit = {
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId == null) {
      logWarning(s"Received preempting event for unknown job $jobId")
      return
    }

    logInfo(s"Armada preempting executor $executorId (job $jobId)")
    // handle decom
    val decommissionInfo = ExecutorDecommissionInfo(
      message = s"Armada preempting (job $jobId)",
      workerHost = None)

    decommissionExecutors(
      Array((executorId, decommissionInfo)),
      adjustTargetNumExecutors = false,
      triggeredByExecutor = false)
  }


  // ========================================================================
  // CUSTOM DRIVER ENDPOINT
  // ========================================================================

  override def createDriverEndpoint(): DriverEndpoint = {
    new ArmadaDriverEndpoint()
  }

  /**
   * Custom driver endpoint with Armada-specific message handling.
   */
  private class ArmadaDriverEndpoint extends DriverEndpoint {

    private val execIdCounter = new AtomicInteger(0)
    private val execIdRequests = new mutable.HashMap[RpcAddress, String]()

    /**
     * Handle dynamic executor ID generation message from executors.
     */
    private def generateExecID(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case GenerateExecID(jobId) =>
        val newId = jobIdToExecutor.get(jobId)
        context.reply(newId)

        val executorAddress = context.senderAddress
        execIdRequests(executorAddress) = newId
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      generateExecID(context)
        .orElse(super.receiveAndReply(context))
    }

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      val execId = addressToExecutorId.get(rpcAddress)
      execId match {
        case Some(id) =>
          executorsPendingDecommission.get(id) match {
            case Some(_) =>
              // Expected disconnection during decommissioning
              logDebug(s"Executor $id disconnected during decommissioning")
              removeExecutor(id, ExecutorDecommission(None))

            case None =>
              // Unexpected disconnection
              logWarning(s"Executor $id disconnected unexpectedly")
              disableExecutor(id)
          }

        case None =>
          execIdRequests.remove(rpcAddress)
      }
    }
  }
}
