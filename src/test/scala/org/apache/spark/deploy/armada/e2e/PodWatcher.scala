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

package org.apache.spark.deploy.armada.e2e

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** Pod status tracking for fail-fast behavior */
case class PodStatusInfo(
    name: String,
    phase: String,
    containerStatuses: Seq[ContainerStatus],
    reason: Option[String] = None
)

case class ContainerStatus(
    name: String,
    ready: Boolean,
    state: String,
    restartCount: Int,
    terminationReason: Option[String] = None
)

/** Captures logs and events for a namespace, only printing on failure */
class LogCapture(namespace: String) {
  private val logs   = mutable.ArrayBuffer[String]()
  private val events = mutable.ArrayBuffer[String]()

  def captureLog(message: String): Unit = synchronized {
    logs += s"[${java.time.Instant.now()}] $message"
  }

  def captureEvent(event: String): Unit = synchronized {
    events += event
  }

  def capturePodLogs(podName: String): Unit = {
    try {
      // Get logs from all containers in the pod with retry
      val cmd =
        Seq("kubectl", "logs", podName, "-n", namespace, "--all-containers=true", "--tail=200")
      val result = KubectlUtils.executeWithRetry(cmd, 10.seconds)
      if (result.exitCode == 0 && result.stdout.nonEmpty) {
        captureLog(s"=== Logs for pod $podName ===\n${result.stdout}")
      }

      // Also try to get previous container logs if the pod restarted
      val prevCmd = Seq(
        "kubectl",
        "logs",
        podName,
        "-n",
        namespace,
        "--previous",
        "--all-containers=true",
        "--tail=100"
      )
      val prevResult = ProcessExecutor.execute(prevCmd, 5.seconds)
      if (prevResult.exitCode == 0 && prevResult.stdout.nonEmpty) {
        captureLog(s"=== Previous logs for pod $podName ===\n${prevResult.stdout}")
      }
    } catch {
      case e: Exception =>
        captureLog(s"Failed to capture logs for $podName: ${e.getMessage}")
    }
  }

  def capturePodDescribe(podName: String): Unit = {
    try {
      val cmd    = Seq("kubectl", "describe", "pod", podName, "-n", namespace)
      val result = ProcessExecutor.execute(cmd, 5.seconds)
      if (result.exitCode == 0 && result.stdout.nonEmpty) {
        // Extract just the events section which is most useful
        val lines       = result.stdout.split("\n")
        val eventsIndex = lines.indexWhere(_.contains("Events:"))
        if (eventsIndex >= 0) {
          val events = lines.drop(eventsIndex).mkString("\n")
          captureLog(s"=== Events for pod $podName ===\n$events")
        } else {
          captureLog(s"=== Describe for pod $podName ===\n${result.stdout}")
        }
      }
    } catch {
      case e: Exception =>
        captureLog(s"Failed to describe pod $podName: ${e.getMessage}")
    }
  }

  def printCapturedLogs(): Unit = synchronized {
    if (logs.nonEmpty || events.nonEmpty) {
      println(s"\n========== CAPTURED LOGS FOR NAMESPACE: $namespace ==========")

      if (events.nonEmpty) {
        println("\n----- Kubernetes Events -----")
        events.foreach(println)
      }

      if (logs.nonEmpty) {
        println("\n----- Pod Logs -----")
        logs.foreach(println)
      }

      println(s"========== END LOGS FOR NAMESPACE: $namespace ==========\n")
    }
  }

  def clear(): Unit = synchronized {
    logs.clear()
    events.clear()
  }
}

/** Watches pods in a namespace and provides fail-fast behavior */
class PodWatcher(
    namespace: String,
    jobSetId: String,
    failFast: Boolean = true
) {
  private val scheduler      = Executors.newSingleThreadScheduledExecutor()
  private val podStatuses    = new ConcurrentHashMap[String, PodStatusInfo]()
  private val capturedEvents = new ConcurrentHashMap[String, Boolean]() // Track events by UID
  private val logCapture     = new LogCapture(namespace)
  private val failurePromise = Promise[String]()
  private var watchTask: Option[ScheduledFuture[_]] = None
  private var stopped                               = false

  def start(): Future[String] = {
    // Capture initial state
    logCapture.captureLog(s"Starting pod monitoring for namespace: $namespace, jobSetId: $jobSetId")

    val task = new Runnable {
      def run(): Unit = {
        if (!stopped) {
          checkPods()
        }
      }
    }

    // Do an initial check immediately
    checkPods()

    // Then continue watching every 3 seconds
    watchTask = Some(scheduler.scheduleAtFixedRate(task, 3, 3, TimeUnit.SECONDS))

    // Return a future that completes when a failure is detected
    failurePromise.future
  }

  private def checkPods(): Unit = {
    try {
      // Get all pods in the namespace with retry
      val cmd    = Seq("kubectl", "get", "pods", "-n", namespace, "-o", "json")
      val result = KubectlUtils.executeWithRetry(cmd, 5.seconds, maxRetries = 2)

      if (result.exitCode == 0) {
        parsePodStatuses(result.stdout).foreach { pod =>
          val previousStatus = podStatuses.get(pod.name)
          podStatuses.put(pod.name, pod)

          // Capture logs when pod transitions to important states
          if (previousStatus == null || previousStatus.phase != pod.phase) {
            pod.phase match {
              case "Running" =>
                // Capture initial logs when pod starts running
                logCapture.captureLog(s"Pod ${pod.name} is now Running")
                // Delay slightly to ensure logs are available
                Thread.sleep(500)
                logCapture.capturePodLogs(pod.name)

              case "Succeeded" =>
                // Capture final logs when pod completes successfully
                logCapture.captureLog(s"Pod ${pod.name} Succeeded")
                logCapture.capturePodLogs(pod.name)

              case "Failed" =>
                // Capture logs when pod fails
                logCapture.captureLog(
                  s"Pod ${pod.name} Failed: ${pod.reason.getOrElse("Unknown reason")}"
                )
                logCapture.capturePodLogs(pod.name)
                logCapture.capturePodDescribe(pod.name)

              case _ => // Pending, Unknown, etc.
            }
          }

          // Also capture logs if container restarts
          pod.containerStatuses.foreach { container =>
            val prevContainer =
              Option(previousStatus).flatMap(_.containerStatuses.find(_.name == container.name))
            val restartIncreased = prevContainer.exists(_.restartCount < container.restartCount)

            if (restartIncreased) {
              logCapture.captureLog(
                s"Container ${container.name} in pod ${pod.name} restarted (count: ${container.restartCount})"
              )
              logCapture.capturePodLogs(pod.name)
            }

            // Capture logs when container terminates with error
            if (container.state == "terminated" && container.terminationReason.isDefined) {
              logCapture.captureLog(
                s"Container ${container.name} terminated: ${container.terminationReason.get}"
              )
              if (
                previousStatus == null || !previousStatus.containerStatuses
                  .exists(c => c.name == container.name && c.state == "terminated")
              ) {
                logCapture.capturePodLogs(pod.name)
              }
            }
          }

          // Check for failures
          if (failFast && isFailure(pod)) {
            handlePodFailure(pod)
          }
        }

        // Capture events periodically
        captureNamespaceEvents()

        // Log summary if there are interesting statuses
        val summary = getStatusSummary()
        if (
          summary.contains("Failed") || summary.contains("Error") ||
          summary.contains("CrashLoopBackOff")
        ) {
          logCapture.captureLog(s"Pod status summary: $summary")
        }
      }
    } catch {
      case e: Exception =>
        logCapture.captureLog(s"Error checking pods: ${e.getMessage}")
    }
  }

  private def parsePodStatuses(json: String): Seq[PodStatusInfo] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    try {
      val data  = mapper.readValue(json, classOf[Map[String, Any]])
      val items = data.getOrElse("items", List()).asInstanceOf[List[Map[String, Any]]]
      items.map { item =>
        val metadata = item("metadata").asInstanceOf[Map[String, Any]]
        val status   = item("status").asInstanceOf[Map[String, Any]]
        val name     = metadata("name").toString
        val phase    = status.getOrElse("phase", "Unknown").toString

        val containerStatuses = status.get("containerStatuses") match {
          case Some(containers: List[Map[String, Any]] @unchecked) =>
            containers.map { c =>
              val containerName = c("name").toString
              val ready         = c.getOrElse("ready", false).asInstanceOf[Boolean]
              val (state, terminationReason) =
                c.get("state").asInstanceOf[Option[Map[String, Any]]] match {
                  case Some(s) =>
                    val stateName = s.keys.head // "running", "terminated", "waiting"
                    val reason = stateName match {
                      case "terminated" =>
                        s.get("terminated").asInstanceOf[Option[Map[String, Any]]].flatMap { term =>
                          term.get("reason").map(_.toString)
                        }
                      case "waiting" =>
                        s.get("waiting").asInstanceOf[Option[Map[String, Any]]].flatMap { wait =>
                          wait.get("reason").map(_.toString)
                        }
                      case _ => None
                    }
                    (stateName, reason)
                  case None => ("unknown", None)
                }
              val restartCount = c.getOrElse("restartCount", 0) match {
                case n: Double => n.toInt
                case n: Int    => n
                case _         => 0
              }
              ContainerStatus(containerName, ready, state, restartCount, terminationReason)
            }
          case _ => Seq.empty
        }

        val reason = status.get("reason").map(_.toString)
        PodStatusInfo(name, phase, containerStatuses, reason)
      }
    } catch {
      case e: Exception =>
        logCapture.captureLog(s"Failed to parse pod statuses: ${e.getMessage}")
        Seq.empty
    }
  }

  private def isFailure(pod: PodStatusInfo): Boolean = {
    // Check if pod itself failed
    if (pod.phase == "Failed") return true

    // Check for pod-level failure reasons
    if (
      pod.reason.exists(r =>
        r.contains("Error") ||
          r.contains("BackOff") ||
          r.contains("OOMKilled") ||
          r.contains("Evicted")
      )
    ) {
      return true
    }

    // Check container states for failures
    pod.containerStatuses.exists { container =>
      // Immediate failure on Error state or CrashLoopBackOff
      container.terminationReason.exists(r =>
        r.contains("Error") ||
          r.contains("OOMKilled") ||
          r.contains("ExitCode")
      ) ||
      // Fail on first crash for driver pods (check for actual failure, not successful completion)
      (pod.name.contains("driver") && container.state == "terminated" &&
        container.terminationReason.exists(r =>
          r.contains("Error") || r.contains("ExitCode") && !r.contains("ExitCode:0")
        )) ||
      // For executors, fail after 2 restarts
      (!pod.name.contains(
        "driver"
      ) && container.state == "terminated" && container.restartCount > 1) ||
      // Catch waiting states like CrashLoopBackOff
      (container.state == "waiting" && container.terminationReason.exists(r =>
        r.contains("CrashLoopBackOff") ||
          r.contains("ImagePullBackOff") ||
          r.contains("ErrImagePull") ||
          r.contains("InvalidImageName")
      ))
    }
  }

  private def handlePodFailure(pod: PodStatusInfo): Unit = {
    if (!failurePromise.isCompleted) {
      val isDriver =
        pod.name.contains("driver") || pod.containerStatuses.exists(_.name.contains("driver"))
      val podType = if (isDriver) "Driver" else "Executor"

      logCapture.captureLog(s"$podType pod failed: ${pod.name}")
      logCapture.captureLog(s"Pod phase: ${pod.phase}, Reason: ${pod.reason.getOrElse("Unknown")}")

      // Capture logs from the failed pod first
      logCapture.capturePodLogs(pod.name)

      // Get pod describe for the failed pod
      try {
        val descCmd    = Seq("kubectl", "describe", "pod", pod.name, "-n", namespace)
        val descResult = ProcessExecutor.execute(descCmd, 5.seconds)
        if (descResult.exitCode == 0) {
          logCapture.captureLog(s"=== Pod describe for ${pod.name} ===\n${descResult.stdout}")
        }
      } catch {
        case _: Exception => // Ignore
      }

      // IMPORTANT: Capture logs from ALL pods in namespace for debugging
      logCapture.captureLog(
        s"\n=== Capturing all pod logs in namespace $namespace for debugging ==="
      )
      captureAllPodsLogs()

      // Capture ALL events in the namespace
      logCapture.captureLog(s"\n=== Capturing all events in namespace $namespace ===")
      captureAllNamespaceEvents()

      val failureMessage = s"$podType pod ${pod.name} failed in namespace $namespace"
      failurePromise.trySuccess(failureMessage)
    }
  }

  private def captureNamespaceEvents(): Unit = {
    try {
      // Get events in JSON format for better parsing
      val cmd =
        Seq("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp", "-o", "json")
      val result = ProcessExecutor.execute(cmd, 5.seconds)
      if (result.exitCode == 0 && result.stdout.nonEmpty) {
        parseAndCaptureEvents(result.stdout)
      }
    } catch {
      case e: Exception =>
        logCapture.captureLog(s"Failed to capture events: ${e.getMessage}")
    }
  }

  private def parseAndCaptureEvents(json: String): Unit = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    try {
      val data  = mapper.readValue(json, classOf[Map[String, Any]])
      val items = data.getOrElse("items", List()).asInstanceOf[List[Map[String, Any]]]

      items.foreach { event =>
        val metadata =
          event.get("metadata").asInstanceOf[Option[Map[String, Any]]].getOrElse(Map.empty)
        val uid = metadata.get("uid").map(_.toString).getOrElse("")

        // Only capture each event once
        if (uid.nonEmpty && !capturedEvents.containsKey(uid)) {
          capturedEvents.put(uid, true)

          val involvedObject =
            event.get("involvedObject").asInstanceOf[Option[Map[String, Any]]].getOrElse(Map.empty)
          val eventType      = event.getOrElse("type", "Normal").toString
          val reason         = event.getOrElse("reason", "").toString
          val message        = event.getOrElse("message", "").toString
          val firstTimestamp = event.get("firstTimestamp").map(_.toString).getOrElse("")
          val lastTimestamp  = event.get("lastTimestamp").map(_.toString).getOrElse("")
          val count = event.get("count") match {
            case Some(n: Double) => n.toInt
            case Some(n: Int)    => n
            case _               => 1
          }

          val objectName = involvedObject.getOrElse("name", "").toString
          val objectKind = involvedObject.getOrElse("kind", "").toString

          // Capture all events, but mark important ones
          val isImportant = eventType == "Warning" || eventType == "Error" ||
            reason.contains("Failed") || reason.contains("Error") ||
            reason.contains("BackOff") || reason.contains("Kill") ||
            reason.contains("OOM") || reason.contains("Evicted") ||
            reason.contains("Unhealthy") || reason.contains("FailedMount")

          val eventInfo = if (count > 1) {
            s"[$eventType] $objectKind/$objectName - $reason: $message (count: $count, last: $lastTimestamp)"
          } else {
            s"[$eventType] $objectKind/$objectName - $reason: $message"
          }

          if (isImportant) {
            logCapture.captureEvent(s"⚠️  $eventInfo")
            // For important events, also capture more context
            if (objectKind == "Pod" && objectName.nonEmpty) {
              logCapture.captureLog(s"Important event for pod $objectName: $reason - $message")
              // Immediately capture pod logs for important events
              if (reason.contains("OOM") || reason.contains("Error") || reason.contains("Failed")) {
                logCapture.capturePodLogs(objectName)
              }
            }
          } else {
            // Still capture normal events, just not marked as important
            logCapture.captureEvent(eventInfo)
          }
        }
      }
    } catch {
      case e: Exception =>
        // Fall back to simple text parsing if JSON parsing fails
        val cmd =
          Seq("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp", "-o", "wide")
        val result = ProcessExecutor.execute(cmd, 5.seconds)
        if (result.exitCode == 0 && result.stdout.nonEmpty) {
          val lines = result.stdout.split("\n")
          lines.foreach(logCapture.captureEvent)
        }
    }
  }

  private def getStatusSummary(): String = {
    val statuses = podStatuses.values().asScala
    val phases = statuses
      .groupBy(_.phase)
      .map { case (phase, pods) =>
        s"$phase=${pods.size}"
      }
      .mkString(", ")
    phases
  }

  private def captureAllPodsLogs(): Unit = {
    try {
      // Get all pods in the namespace
      val cmd    = Seq("kubectl", "get", "pods", "-n", namespace, "-o", "name")
      val result = ProcessExecutor.execute(cmd, 5.seconds)
      if (result.exitCode == 0 && result.stdout.nonEmpty) {
        val podNames = result.stdout.split("\n").map(_.replace("pod/", "").trim).filter(_.nonEmpty)

        logCapture.captureLog(s"Found ${podNames.length} pods in namespace $namespace")

        podNames.foreach { podName =>
          try {
            logCapture.captureLog(s"\n=== Logs for pod $podName ===")
            logCapture.capturePodLogs(podName)

            // Also capture describe for each pod
            val descCmd    = Seq("kubectl", "describe", "pod", podName, "-n", namespace)
            val descResult = ProcessExecutor.execute(descCmd, 5.seconds)
            if (descResult.exitCode == 0) {
              // Just capture the events section from describe
              val lines       = descResult.stdout.split("\n")
              val eventsIndex = lines.indexWhere(_.contains("Events:"))
              if (eventsIndex >= 0) {
                val events = lines.drop(eventsIndex).take(30).mkString("\n")
                logCapture.captureLog(s"=== Events for pod $podName ===\n$events")
              }
            }
          } catch {
            case e: Exception =>
              logCapture.captureLog(s"Failed to capture logs for pod $podName: ${e.getMessage}")
          }
        }
      }
    } catch {
      case e: Exception =>
        logCapture.captureLog(s"Failed to list pods: ${e.getMessage}")
    }
  }

  private def captureAllNamespaceEvents(): Unit = {
    try {
      // Get ALL events in the namespace, not just recent ones
      val cmd =
        Seq("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp", "-o", "wide")
      val result = ProcessExecutor.execute(cmd, 10.seconds)
      if (result.exitCode == 0 && result.stdout.nonEmpty) {
        logCapture.captureLog("=== All Namespace Events ===")
        logCapture.captureLog(result.stdout)
      }
    } catch {
      case e: Exception =>
        logCapture.captureLog(s"Failed to capture all events: ${e.getMessage}")
    }
  }

  def stop(): Unit = {
    stopped = true
    watchTask.foreach(_.cancel(false))
    scheduler.shutdown()
  }

  def getLogCapture(): LogCapture = logCapture

  def getFailure(): Option[String] = {
    if (failurePromise.isCompleted) {
      Some(failurePromise.future.value.get.get)
    } else {
      None
    }
  }
}
