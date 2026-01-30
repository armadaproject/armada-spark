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

import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal
import scala.concurrent.Await
import scala.concurrent.duration._

import api.event.{
  EventGrpc,
  EventMessage,
  JobSubmittedEvent,
  JobQueuedEvent,
  JobPendingEvent,
  JobRunningEvent,
  JobSucceededEvent,
  JobFailedEvent,
  JobCancelledEvent,
  JobPreemptingEvent,
  JobPreemptedEvent,
  JobTerminatedEvent
}
import io.grpc.ManagedChannel
import org.apache.spark.internal.Logging

/** Keeps track of the armada event stream for the jobset
  */
private[spark] class ArmadaEventWatcher(
    grpcChannel: ManagedChannel,
    queue: String,
    jobSetId: String,
    backend: ArmadaClusterManagerBackend,
    jobIdToExecutor: ConcurrentHashMap[String, String],
    submitClient: io.armadaproject.armada.ArmadaClient
) extends Logging {

  @volatile private var running = false
  private val watcherThread = new Thread("armada-event-watcher") {
    setDaemon(true)
    override def run(): Unit = watchEvents()
  }

  // Lifecycle
  def start(): Unit = {
    logInfo(s"Starting Armada event watcher for job set: $jobSetId")
    running = true
    watcherThread.start()
  }

  def stop(): Unit = {
    logInfo("Stopping Armada event watcher")
    running = false
    watcherThread.interrupt()
    try {
      watcherThread.join(5000)
    } catch {
      case _: InterruptedException =>
      // Expected when stopping
      case NonFatal(e) =>
        // Suppress any other errors during shutdown (including gRPC errors)
        logDebug(s"Error stopping watcher thread: ${e.getMessage}")
    }
  }

  // Streaming loop
  private def watchEvents(): Unit = {
    var lastMessageId: String = ""

    // Ensure submit service is healthy before starting to watch events
    waitForSubmitHealth()

    while (running) {
      try {
        val watcher = submitClient.jobWatcher(queue, jobSetId, lastMessageId)

        while (running && watcher.hasNext) {
          try {
            val streamMessage = watcher.next()
            if (streamMessage != null) {
              lastMessageId = streamMessage.id
              streamMessage.message.foreach(processEventMessage)
            }
          } catch {
            case _: InterruptedException =>
              logInfo("Event stream interrupted")
              running = false
            case NonFatal(e) =>
              logWarning(s"Error processing event: ${e.getMessage}", e)
          }
        }

        if (running) {
          logWarning("Event stream ended, reconnecting...")
          Thread.sleep(1000)
        }
      } catch {
        case _: InterruptedException =>
          logInfo("Event watcher interrupted")
          running = false
        case NonFatal(e) =>
          logWarning(s"Error in event stream: ${e.getMessage}", e)
          if (running) Thread.sleep(5000)
      }
    }

    logInfo("Event watcher stopped")
  }

  private def waitForSubmitHealth(): Unit = {
    val timeout = 10.seconds
    while (running) {
      try {
        logInfo(s"Checking Armada submit service health (timeout: $timeout)...")
        val resp = Await.result(submitClient.submitHealth(), timeout)
        if (resp.status.isServing) {
          logInfo("Armada submit service is healthy; starting event watch.")
          return
        } else {
          logWarning("Armada submit service is not serving; retrying in 5s...")
          Thread.sleep(5000)
        }
      } catch {
        case _: InterruptedException =>
          running = false
          return
        case NonFatal(e) =>
          logWarning(s"Submit health check failed: ${e.getMessage}", e)
          Thread.sleep(5000)
      }
    }
  }

  private def processEventMessage(eventMessage: EventMessage): Unit = {
    eventMessage.events match {
      case EventMessage.Events.Submitted(event) =>
        handleSubmitted(event)

      case EventMessage.Events.Queued(event) =>
        handleQueued(event)

      case EventMessage.Events.Pending(event) =>
        handlePending(event)

      case EventMessage.Events.Running(event) =>
        handleRunning(event)

      case EventMessage.Events.Succeeded(event) =>
        handleSucceeded(event)

      case EventMessage.Events.Failed(event) =>
        handleFailed(event)

      case EventMessage.Events.Cancelled(event) =>
        handleCancelled(event)

      case EventMessage.Events.Preempting(event) =>
        handlePreempting(event)

      case EventMessage.Events.Preempted(event) =>
        handlePreempted(event)

      case EventMessage.Events.Leased(_) =>
        logInfo("Job leased event received")

      case EventMessage.Events.LeaseReturned(_) =>
        logInfo("Job lease returned event received")

      case EventMessage.Events.LeaseExpired(_) =>
        logInfo("Job lease expired event received")

      case EventMessage.Events.Reprioritized(_) =>
        logInfo("Job reprioritized event received")

      case EventMessage.Events.Reprioritizing(_) =>
        logInfo("Job reprioritizing event received")

      case EventMessage.Events.Cancelling(_) =>
        logInfo("Job cancelling event received")

      case EventMessage.Events.Utilisation(_) =>
        logInfo("Job utilisation event received")

      case EventMessage.Events.IngressInfo(_) =>
        logInfo("Job ingress info event received")

      case EventMessage.Events.Empty =>
        logInfo("Empty event received")
    }
  }

  private def handleSubmitted(event: JobSubmittedEvent): Unit = {
    val jobId = event.jobId
    backend.onExecutorSubmitted(jobId)
    logInfo(s"[Armada] Job submitted (jobId=$jobId)")
  }

  private def handleQueued(event: JobQueuedEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) queued in Armada")
    }
  }

  private def handlePending(event: JobPendingEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) pending (pod creating)")
    }
  }

  private def handleRunning(event: JobRunningEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) is running")
      backend.onExecutorRunning(jobId, executorId)
    } else {
      logWarning(s"Received running event for unknown job $jobId")
    }
  }

  private def handleSucceeded(event: JobSucceededEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      // Executors should not succeed normally (they're long-running)
      logWarning(s"Executor $executorId (job $jobId) succeeded unexpectedly")
      backend.onExecutorFailed(jobId, executorId, 0, "Unexpected success")
    }
  }

  private def handleFailed(event: JobFailedEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      val exitCode   = event.containerStatuses.headOption.map(_.exitCode).getOrElse(-1)
      val reason     = Option(event.reason).filter(_.nonEmpty).getOrElse("Unknown failure")
      val causeStr   = Option(event.cause).map(c => s"Cause: ${c.name}").getOrElse("")
      val fullReason = Seq(reason, causeStr).filter(s => s != null && s.nonEmpty).mkString(", ")
      logWarning(s"Executor $executorId (job $jobId) failed: $fullReason (exit code: $exitCode)")
      backend.onExecutorFailed(jobId, executorId, exitCode, fullReason)
    } else {
      logInfo(s"Received failed event for unknown job $jobId")
    }
  }

  private def handleCancelled(event: JobCancelledEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      val reason = Option(event.reason).filter(_.nonEmpty).getOrElse("Job cancelled")
      logInfo(s"Executor $executorId (job $jobId) cancelled: $reason")
      backend.onExecutorCancelled(jobId, executorId)
    }
  }

  private def handlePreempting(event: JobPreemptingEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) is being preempted")
      backend.onArmadaPreempting(jobId)
    } else {
      logWarning(s"Received preempting event for unknown job $jobId")
    }
  }

  private def handlePreempted(event: JobPreemptedEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) was preempted")
      backend.onExecutorFailed(jobId, executorId, -1, "Preempted by Armada")
    }
  }

  private def handleTerminated(event: JobTerminatedEvent): Unit = {
    val jobId      = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) terminated")
    }
  }

}
