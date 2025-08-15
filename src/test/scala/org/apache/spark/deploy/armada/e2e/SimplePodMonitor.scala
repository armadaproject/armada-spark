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

import scala.collection.mutable
import scala.concurrent.duration._

/** Simple pod monitoring that captures logs and events on failure */
class SimplePodMonitor(namespace: String) {
  private val capturedLogs = mutable.ArrayBuffer[String]()

  /** Check if any pods have failed and capture their logs if so */
  def checkForFailures(): Option[String] = {
    try {
      val podsCmd    = Seq("kubectl", "get", "pods", "-n", namespace, "-o", "wide")
      val podsResult = ProcessExecutor.execute(podsCmd, 10.seconds)

      if (podsResult.exitCode == 0) {
        val lines = podsResult.stdout.split("\n")

        val failedPods = lines.drop(1).filter { line =>
          line.contains("Failed") ||
          line.contains("Error") ||
          line.contains("CrashLoopBackOff") ||
          line.contains("OOMKilled") ||
          line.contains("Evicted")
        }

        if (failedPods.nonEmpty) {
          val failedPodName = failedPods.head.split("\\s+").head

          // Immediately capture logs for the failed pod
          println(s"[MONITOR] Pod $failedPodName failed, capturing logs...")
          try {
            val logsCmd = Seq(
              "kubectl",
              "logs",
              failedPodName,
              "-n",
              namespace,
              "--all-containers=true",
              "--tail=100"
            )
            val logsResult = ProcessExecutor.executeWithResult(logsCmd, 10.seconds)
            if (logsResult.exitCode == 0 && logsResult.stdout.nonEmpty) {
              println(s"[MONITOR] Pod $failedPodName logs:")
              println(logsResult.stdout)
            }

            // Also try to describe the pod
            val describeCmd    = Seq("kubectl", "describe", "pod", failedPodName, "-n", namespace)
            val describeResult = ProcessExecutor.executeWithResult(describeCmd, 10.seconds)
            if (describeResult.exitCode == 0) {
              val lines       = describeResult.stdout.split("\n")
              val eventsIndex = lines.indexWhere(_.contains("Events:"))
              if (eventsIndex >= 0) {
                println(s"[MONITOR] Pod $failedPodName events:")
                println(lines.slice(eventsIndex, eventsIndex + 20).mkString("\n"))
              }
            }
          } catch {
            case e: Exception =>
              println(s"[MONITOR] Failed to capture logs for $failedPodName: ${e.getMessage}")
          }

          Some(s"Pod $failedPodName failed in namespace $namespace")
        } else {
          None
        }
      } else {
        println(s"[MONITOR] Failed to get pods: ${podsResult.stderr}")
        None
      }
    } catch {
      case e: Exception =>
        println(s"[MONITOR] Error checking pods: ${e.getMessage}")
        None
    }
  }

  /** Capture all logs and events for debugging */
  def captureDebugInfo(): Unit = {
    println(s"\n[DEBUG] Capturing debug info for namespace $namespace")

    try {
      val podsCmd    = Seq("kubectl", "get", "pods", "-n", namespace, "-o", "name")
      val podsResult = ProcessExecutor.execute(podsCmd, 10.seconds)

      if (podsResult.exitCode == 0) {
        val podNames =
          podsResult.stdout.split("\n").map(_.replace("pod/", "").trim).filter(_.nonEmpty)

        podNames.foreach { podName =>
          try {
            println(s"[DEBUG] Capturing logs for pod $podName")

            val logsCmd = Seq(
              "kubectl",
              "logs",
              podName,
              "-n",
              namespace,
              "--all-containers=true",
              "--tail=200"
            )
            val logsResult = ProcessExecutor.execute(logsCmd, 10.seconds)
            if (logsResult.exitCode == 0 && logsResult.stdout.nonEmpty) {
              capturedLogs += s"\n=== Logs for pod $podName ===\n${logsResult.stdout}"
            }

            val prevLogsCmd = Seq(
              "kubectl",
              "logs",
              podName,
              "-n",
              namespace,
              "--previous",
              "--all-containers=true",
              "--tail=100"
            )
            val prevResult = ProcessExecutor.executeWithResult(prevLogsCmd, 5.seconds)
            if (prevResult.exitCode == 0 && prevResult.stdout.nonEmpty) {
              capturedLogs += s"\n=== Previous logs for pod $podName ===\n${prevResult.stdout}"
            }

            val describeCmd    = Seq("kubectl", "describe", "pod", podName, "-n", namespace)
            val describeResult = ProcessExecutor.execute(describeCmd, 5.seconds)
            if (describeResult.exitCode == 0) {
              val lines       = describeResult.stdout.split("\n")
              val eventsIndex = lines.indexWhere(_.contains("Events:"))
              if (eventsIndex >= 0) {
                val events = lines.drop(eventsIndex).mkString("\n")
                capturedLogs += s"\n=== Events for pod $podName ===\n$events"
              }
            }
          } catch {
            case e: Exception =>
              println(s"[DEBUG] Failed to capture info for pod $podName: ${e.getMessage}")
          }
        }
      }

      val eventsCmd = Seq("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
      val eventsResult = ProcessExecutor.execute(eventsCmd, 10.seconds)
      if (eventsResult.exitCode == 0 && eventsResult.stdout.nonEmpty) {
        capturedLogs += s"\n=== All Namespace Events ===\n${eventsResult.stdout}"
      }

    } catch {
      case e: Exception =>
        println(s"[DEBUG] Failed to capture debug info: ${e.getMessage}")
    }
  }

  /** Print all captured logs */
  def printCapturedLogs(): Unit = {
    if (capturedLogs.nonEmpty) {
      println(s"\n========== DEBUG INFO FOR NAMESPACE: $namespace ==========")
      capturedLogs.foreach(println)
      println(s"========== END DEBUG INFO ==========\n")
    }
  }
}
