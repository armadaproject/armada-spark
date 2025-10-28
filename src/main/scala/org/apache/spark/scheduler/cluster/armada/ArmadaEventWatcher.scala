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

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}

import scala.util.control.NonFatal

import api.event.{EventGrpc, EventStreamMessage, WatchRequest, EventMessage, JobSubmittedEvent, JobQueuedEvent, JobPendingEvent, JobRunningEvent, JobSucceededEvent, JobFailedEvent, JobCancelledEvent, JobPreemptingEvent, JobPreemptedEvent, JobTerminatedEvent}
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver
import org.apache.spark.internal.Logging

/**
 * Minimal Armada event watcher placeholder.
 *
 * This class is intended to be started by ArmadaClusterManagerBackend.
 * For now, it starts a daemon thread and keeps it alive until stopped.
 * The actual event handling will be implemented with the current Armada Scala client API.
 */
private[spark] class ArmadaEventWatcher(
    grpcChannel: ManagedChannel,
    queue: String,
    jobSetId: String,
    backend: ArmadaClusterManagerBackend,
    jobIdToExecutor: ConcurrentHashMap[String, String]) extends Logging {

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
    try watcherThread.join(5000) catch { case _: InterruptedException => () }
  }

  // Streaming loop
  private def watchEvents(): Unit = {
    var lastMessageId: String = ""

    while (running) {
      try {
        logInfo(s"Connecting to Armada event stream: queue=$queue, jobSetId=$jobSetId")

        val eventStub = EventGrpc.stub(grpcChannel)
        val watchRequest = WatchRequest(queue = queue, jobSetId = jobSetId, fromId = lastMessageId)

        val eventQueue = new LinkedBlockingQueue[EventStreamMessage]()
        @volatile var streamCompleted = false
        val responseObserver = new StreamObserver[EventStreamMessage] {
          override def onNext(value: EventStreamMessage): Unit = eventQueue.put(value)
          override def onError(t: Throwable): Unit = {
            logWarning(s"Armada event stream error: ${t.getMessage}", t)
            streamCompleted = true
          }
          override def onCompleted(): Unit = {
            logInfo("Armada event stream completed")
            streamCompleted = true
          }
        }

        // Start streaming
        eventStub.watch(watchRequest, responseObserver)
        logInfo("Connected to Armada event stream")

        while (running && !streamCompleted) {
          try {
            val streamMessage = eventQueue.poll(1, TimeUnit.SECONDS)
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
          Thread.sleep(5000)
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
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) submitted to Armada")
    } else {
      logInfo(s"[Armada] Job submitted (jobId=$jobId)")
    }
  }

  private def handleQueued(event: JobQueuedEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) queued in Armada")
    }
  }

  private def handlePending(event: JobPendingEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) pending (pod creating)")
    }
  }

  private def handleRunning(event: JobRunningEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) is running")
      backend.onExecutorRunning(jobId, executorId)
    } else {
      logWarning(s"Received running event for unknown job $jobId")
    }
  }

  private def handleSucceeded(event: JobSucceededEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      // Executors should not succeed normally (they're long-running)
      logWarning(s"Executor $executorId (job $jobId) succeeded unexpectedly")
      backend.onExecutorFailed(jobId, executorId, 0, "Unexpected success")
    }
  }

  private def handleFailed(event: JobFailedEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      val exitCode = event.containerStatuses.headOption.map(_.exitCode).getOrElse(-1)
      val reason = Option(event.reason).filter(_.nonEmpty).getOrElse("Unknown failure")
      val causeStr = Option(event.cause).map(c => s"Cause: ${c.name}").getOrElse("")
      val fullReason = Seq(reason, causeStr).filter(s => s != null && s.nonEmpty).mkString(", ")
      logWarning(s"Executor $executorId (job $jobId) failed: $fullReason (exit code: $exitCode)")
      backend.onExecutorFailed(jobId, executorId, exitCode, fullReason)
    } else {
      logInfo(s"Received failed event for unknown job $jobId")
    }
  }

  private def handleCancelled(event: JobCancelledEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      val reason = Option(event.reason).filter(_.nonEmpty).getOrElse("Job cancelled")
      logInfo(s"Executor $executorId (job $jobId) cancelled: $reason")
      backend.onExecutorCancelled(jobId, executorId)
    }
  }

  private def handlePreempting(event: JobPreemptingEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) is being preempted")
      backend.onArmadaPreempting(jobId)
    } else {
      logWarning(s"Received preempting event for unknown job $jobId")
    }
  }

  private def handlePreempted(event: JobPreemptedEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) was preempted")
      backend.onExecutorFailed(jobId, executorId, -1, "Preempted by Armada")
    }
  }

  private def handleTerminated(event: JobTerminatedEvent): Unit = {
    val jobId = event.jobId
    val executorId = jobIdToExecutor.get(jobId)
    if (executorId != null) {
      logInfo(s"Executor $executorId (job $jobId) terminated")
    }
  }

}
