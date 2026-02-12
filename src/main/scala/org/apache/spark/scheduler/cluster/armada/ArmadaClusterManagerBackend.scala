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
import scala.concurrent.Future

import api.submit.JobCancelRequest
import io.armadaproject.armada.ArmadaClient
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.MetadataUtils
import io.grpc.Metadata

import org.apache.spark.SparkContext
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.deploy.armada.DeploymentModeHelper
import org.apache.spark.deploy.armada.submit.ArmadaUtils
import org.apache.spark.internal.config.SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}
import org.apache.spark.scheduler.{
  ExecutorDecommission,
  ExecutorDecommissionInfo,
  ExecutorExited,
  TaskSchedulerImpl
}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.scheduler.cluster.k8s.GenerateExecID
import org.apache.spark.util.{ThreadUtils, Utils}

/** Spark scheduler backend implementation for Armada.
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

  private val execIdCounter = new AtomicInteger(0)

  /** Maps Spark executor ID to Armada job ID */
  private val executorToJobId = new ConcurrentHashMap[String, String]()

  /** Maps Armada job ID to Spark executor ID */
  private val jobIdToExecutor = new ConcurrentHashMap[String, String]()

  /** Tracks requested executors not yet seen in events */
  private val pendingExecutors = new mutable.HashSet[String]()

  /** Tracks executors that have reached a terminal state (succeeded, failed, cancelled) */
  private val terminalExecutors: java.util.Set[String] =
    ConcurrentHashMap.newKeySet[String]()

  /** Initial executor count */
  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  /** Minimum registered ratio for starting */
  protected override val minRegisteredRatio: Double =
    if (conf.get(SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  /** Armada connection details */
  private var armadaClient: Option[ArmadaClient]       = None
  private var eventWatcher: Option[ArmadaEventWatcher] = None
  private var queue: Option[String]                    = None
  private var jobSetId: Option[String]                 = None
  private var namespace: Option[String]                = None

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

    // Set default application ID if not already set (needed for client mode where ArmadaClientApplication doesn't run)
    ArmadaUtils.setDefaultAppId(conf)

    // Initialize Armada event watcher if queue and jobSetId are provided
    val queueOpt    = conf.get(ARMADA_JOB_QUEUE)
    val modeHelper  = DeploymentModeHelper(conf)
    val jobSetIdOpt = modeHelper.getJobSetIdSource(applicationId())

    (queueOpt, jobSetIdOpt) match {
      case (Some(q), Some(jsId)) =>
        try {
          queue = Some(q)
          jobSetId = Some(jsId)
          namespace = conf.get(ARMADA_SPARK_JOB_NAMESPACE)

          val serverUrl    = conf.get(ARMADA_SERVER_INTERNAL_URL).getOrElse(masterURL)
          val (host, port) = ArmadaUtils.parseMasterUrl(serverUrl)

          logInfo(
            s"Starting Armada cluster scheduler backend: " +
              s"jobSetId=$jsId, queue=$q, namespace=$namespace"
          )

          // Initialize Armada client with auth token
          val token  = ArmadaUtils.getAuthToken(Some(conf))
          val client = ArmadaClient(host, port, useSsl = false, token)
          armadaClient = Some(client)

          createWatcher(q, jsId, host, port, client, token)

          createAllocator(q, jsId, client)

          // proactively request executors in static client mode only
          // Additional check: check for ARMADA_JOB_SET_ID env var (only set in cluster mode)
          val isClusterModeEnvCheck = sys.env.contains("ARMADA_JOB_SET_ID")
          val shouldProactivelyRequest =
            !isClusterModeEnvCheck && modeHelper.shouldProactivelyRequestExecutors && initialExecutors > 0

          if (shouldProactivelyRequest) {
            val executorCount  = modeHelper.getExecutorCount
            val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(conf)
            doRequestTotalExecutors(Map(defaultProfile -> executorCount))
          }

        } catch {
          case e: Throwable =>
            logWarning(s"Failed to start Armada components: ${e.getMessage}", e)
        }

      case _ =>
        logInfo("Armada queue or jobSetId not configured; backend started in passive mode")
    }
  }

  private def createAllocator(q: String, jsId: String, client: ArmadaClient): Unit = {
    val allocator = new ArmadaExecutorAllocator(
      client,
      q,
      jsId,
      conf,
      applicationId(),
      this
    )
    executorAllocator = Some(allocator)
    allocator.start()
  }

  private def createWatcher(
      q: String,
      jsId: String,
      host: String,
      port: Int,
      client: ArmadaClient,
      token: Option[String]
  ): Unit = {
    // Configure TLS
    val useTls         = conf.get(ARMADA_EVENT_WATCHER_USE_TLS)
    val channelBuilder = NettyChannelBuilder.forAddress(host, port)

    val channelBuilderWithTls = if (useTls) {
      logInfo("Using TLS for event watcher gRPC channel")
      channelBuilder.useTransportSecurity()
    } else {
      logInfo("Using plaintext for event watcher gRPC channel")
      channelBuilder.usePlaintext()
    }

    val channel = token match {
      case Some(t) =>
        val metadata = new Metadata()
        metadata.put(
          Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
          "Bearer " + t
        )
        channelBuilderWithTls
          .intercept(MetadataUtils.newAttachHeadersInterceptor(metadata))
          .build()
      case None =>
        channelBuilderWithTls.build()
    }

    // Start event watcher
    val watcher = new ArmadaEventWatcher(q, jsId, this, jobIdToExecutor, client)
    eventWatcher = Some(watcher)
    watcher.start()
    logInfo(s"Armada Event Watcher started for queue=$q jobSetId=$jsId at $host:$port")
  }

  // Update the executor data strucs with the new executor
  private[spark] def recordExecutor(jobId: String): String = {
    jobIdToExecutor.synchronized {
      if (!jobIdToExecutor.containsKey(jobId)) {
        val newId = execIdCounter.incrementAndGet().toString
        // Register mapping
        jobIdToExecutor.put(jobId, newId)
        executorToJobId.put(newId, jobId)
        newId

      } else {
        jobIdToExecutor.get(jobId)
      }
    }
  }

  override def stop(): Unit = {
    logInfo("Stopping Armada cluster scheduler backend")

    // Call parent stop - sends StopExecutor RPCs to all executors
    Utils.tryLogNonFatalError {
      super.stop()
    }

    // Stop executor allocator (no new executors needed during shutdown)
    Utils.tryLogNonFatalError {
      executorAllocator.foreach(_.stop())
    }

    // Cancel non-terminal executor jobs if configured.
    // The event watcher stays running during the grace period so it can
    // receive terminal events (Succeeded/Failed) and mark executors in
    // terminalExecutors. This allows executors that exit gracefully after
    // receiving StopExecutor to show "Succeeded" in Armada instead of
    // "Cancelled".
    if (conf.get(ARMADA_DELETE_EXECUTORS)) {
      val gracePeriod = conf.get(ARMADA_KILL_GRACE_PERIOD)
      logInfo(s"Waiting ${gracePeriod}ms for executors to exit gracefully")
      try { Thread.sleep(gracePeriod) }
      catch { case _: InterruptedException => }

      Utils.tryLogNonFatalError {
        cancelExecutorJobs()
      }
    }

    // Stop event watcher after grace period so terminal events are captured
    Utils.tryLogNonFatalError {
      eventWatcher.foreach(_.stop())
    }

    // Shutdown executor service
    Utils.tryLogNonFatalError {
      ThreadUtils.shutdown(executorService)
    }

    eventWatcher = None
    armadaClient = None
    executorAllocator = None
  }

  /** Cancel Armada jobs for the given executor IDs.
    *
    * @param executorIds
    *   Spark executor IDs to cancel
    * @param reason
    *   Reason for cancellation
    */
  private def cancelArmadaJobs(executorIds: Seq[String], reason: String): Unit = {
    (armadaClient, queue, jobSetId) match {
      case (Some(client), Some(q), Some(jsId)) =>
        // Map executor IDs to Armada job IDs
        val jobIds = executorIds.flatMap(id => Option(executorToJobId.get(id)))

        if (jobIds.nonEmpty) {
          val cancelRequest = JobCancelRequest(
            jobSetId = jsId,
            jobId = "",
            jobIds = jobIds,
            queue = q,
            reason = reason
          )

          client.cancelJobs(cancelRequest)
          logInfo(s"Cancelled ${jobIds.size} Armada jobs: $reason")
        } else {
          logDebug(s"No Armada jobs to cancel")
        }
      case _ =>
        logWarning("Cannot cancel jobs: Armada client not initialized")
    }
  }

  /** Cancel all non-terminal executor jobs (driver is already filtered out at submission time).
    */
  private def cancelExecutorJobs(): Unit = {
    val executorIds = getActiveExecutorIds()

    if (executorIds.nonEmpty) {
      cancelArmadaJobs(executorIds, "Spark application stopping - cancelling executors")
    } else {
      logInfo(s"No executor jobs to cancel")
    }
  }

  // ========================================================================
  // EXECUTOR ALLOCATION
  // ========================================================================

  // Called by spark core to set the number of executors that should be running
  override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]
  ): Future[Boolean] = {
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

  // Called by spark core to reduce the number of executors that should be running
  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    logInfo(s"Killing ${executorIds.size} executors: ${executorIds.mkString(", ")}")

    // Send RPC kill signal to executors
    executorIds.foreach { id =>
      markTerminal(id)
      safeRemoveExecutor(
        id,
        ExecutorExited(-1, exitCausedByApp = false, "Executor killed by Spark")
      )
    }

    // Cancel Armada jobs after grace period
    val killTask = new Runnable() {
      override def run(): Unit = Utils.tryLogNonFatalError {
        cancelArmadaJobs(executorIds, "Executor killed by Spark")
      }
    }

    executorService.schedule(killTask, conf.get(ARMADA_KILL_GRACE_PERIOD), TimeUnit.MILLISECONDS)

    Future.successful(true)
  }

  // ========================================================================
  // DECOMMISSIONING SUPPORT
  // ========================================================================

  override def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean
  ): Seq[String] = {

    val executorIds = executorsAndDecomInfo.map(_._1)
    logInfo(s"Decommissioning ${executorIds.length} executors: ${executorIds.mkString(", ")}")

    // Delegate to parent - it handles everything via BlockManagerDecommissioner
    super.decommissionExecutors(
      executorsAndDecomInfo,
      adjustTargetNumExecutors,
      triggeredByExecutor
    )
  }

  // ========================================================================
  // ARMADA-SPECIFIC EVENT HANDLERS
  // ========================================================================

  /** Called by event watcher when executor job is running
    */
  private[armada] def onExecutorRunning(jobId: String, executorId: String): Unit = {
    logInfo(s"Executor $executorId (job $jobId) is running")
  }

  /** Called by event watcher when executor job fails
    */
  private[armada] def onExecutorFailed(
      jobId: String,
      executorId: String,
      exitCode: Int,
      reason: String
  ): Unit = {

    val exitReason = ExecutorExited(
      exitCode,
      exitCausedByApp = exitCode != 0,
      s"Armada job $jobId failed: $reason"
    )

    markTerminal(executorId)
    safeRemoveExecutor(executorId, exitReason)
  }

  /** Called by event watcher when executor job is cancelled
    */
  private[armada] def onExecutorCancelled(jobId: String, executorId: String): Unit = {
    val exitReason = ExecutorExited(-1, exitCausedByApp = false, s"Armada job $jobId was cancelled")

    markTerminal(executorId)
    safeRemoveExecutor(executorId, exitReason)
  }

  /** Called by event watcher when executor job succeeds
    */
  private[armada] def onExecutorSucceeded(jobId: String, executorId: String): Unit = {
    markTerminal(executorId)
    val exitReason = ExecutorExited(
      0,
      exitCausedByApp = false,
      s"Armada job $jobId succeeded"
    )
    safeRemoveExecutor(executorId, exitReason)
  }

  /** Returns executor IDs that are NOT in a terminal state.
    */
  private[armada] def getActiveExecutorIds(): Seq[String] = {
    executorToJobId.synchronized {
      executorToJobId.asScala.keys.filterNot(terminalExecutors.contains).toSeq
    }
  }

  /** Best-effort removeExecutor that tolerates RPC endpoint being gone during shutdown. During the
    * grace period the CoarseGrainedScheduler endpoint may already be stopped, so removeExecutor can
    * throw. The critical state (markTerminal) is always set before this is called.
    */
  private def safeRemoveExecutor(executorId: String, reason: ExecutorExited): Unit = {
    Utils.tryLogNonFatalError {
      removeExecutor(executorId, reason)
    }
  }

  /** Mark an executor as having reached a terminal state and clean it from pending set.
    */
  private def markTerminal(executorId: String): Unit = {
    pendingExecutors.synchronized {
      pendingExecutors -= executorId
      terminalExecutors.add(executorId)
    }
  }

  private[armada] def onExecutorSubmitted(jobId: String): Unit = {
    // Filter out driver job - only track executor jobs
    val driverJobId = sys.env.get("ARMADA_JOB_ID")
    if (driverJobId.contains(jobId)) {
      return
    }

    val execId = recordExecutor(jobId)
    pendingExecutors.synchronized { pendingExecutors += execId }
  }

  // ========================================================================
  // PENDING EXECUTORS MANAGEMENT
  // ========================================================================

  /** Add an executor to the pending set.
    */
  private[armada] def addPendingExecutor(executorId: String): Unit = {
    pendingExecutors.synchronized {
      pendingExecutors += executorId
    }
  }

  /** Get the count of pending executors. Excludes executors that have already registered with
    * Spark.
    */
  private[armada] def getPendingExecutorCount: Int = {
    pendingExecutors.synchronized {
      val registeredIds = getExecutorIds().toSet
      pendingExecutors --= registeredIds
      pendingExecutors.size
    }
  }

  /** Called when Armada signals a job is being preempted. Proactively start decommissioning.
    */
  private[armada] def onArmadaPreempting(jobId: String): Unit = {
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId == null) {
      logWarning(s"Received preempting event for unknown job $jobId")
      return
    }

    logInfo(s"Armada preempting executor $executorId (job $jobId)")
    // handle decom
    val decommissionInfo =
      ExecutorDecommissionInfo(message = s"Armada preempting (job $jobId)", workerHost = None)

    decommissionExecutors(
      Array((executorId, decommissionInfo)),
      adjustTargetNumExecutors = false,
      triggeredByExecutor = false
    )
  }

  /** Called when Armada is unable to schedule a job.
    */
  private[armada] def onExecutorUnableToSchedule(
      jobId: String,
      executorId: String,
      reason: String
  ): Unit = {

    val exitReason =
      ExecutorExited(-1, exitCausedByApp = false, s"Armada job $jobId unable to schedule: $reason")

    markTerminal(executorId)
    safeRemoveExecutor(executorId, exitReason)
  }

  // ========================================================================
  // CUSTOM DRIVER ENDPOINT
  // ========================================================================

  override def createDriverEndpoint(): DriverEndpoint = {
    new ArmadaDriverEndpoint()
  }

  /** Custom driver endpoint with Armada-specific message handling.
    */
  private class ArmadaDriverEndpoint extends DriverEndpoint {

    private val execIdRequests = new mutable.HashMap[RpcAddress, String]()

    /** Handle dynamic executor ID generation message from executors.
      */
    private def generateExecID(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case GenerateExecID(jobId) =>
        val newId = recordExecutor(jobId)
        context.reply(newId)

        val executorAddress = context.senderAddress
        execIdRequests(executorAddress) = newId
        logDebug(s"Assigned executor ID $newId to Armada job $jobId")
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
