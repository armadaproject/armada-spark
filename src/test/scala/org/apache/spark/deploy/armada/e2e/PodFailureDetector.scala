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

/** Simplified pod failure detection logic */
object PodFailureDetector {

  sealed trait FailureReason
  case object PodFailed         extends FailureReason
  case object ContainerError    extends FailureReason
  case object OOMKilled         extends FailureReason
  case object ImagePullError    extends FailureReason
  case object CrashLoopBackOff  extends FailureReason
  case object Evicted           extends FailureReason
  case object NonZeroExit       extends FailureReason
  case object ExcessiveRestarts extends FailureReason

  def detectFailure(pod: PodStatusInfo): Option[FailureReason] = {
    // Check pod-level failures first
    podLevelFailure(pod) orElse containerLevelFailure(pod)
  }

  private def podLevelFailure(pod: PodStatusInfo): Option[FailureReason] = {
    if (pod.phase == "Failed") {
      return Some(PodFailed)
    }

    pod.reason.flatMap { reason =>
      reason match {
        case r if r.contains("OOMKilled")                      => Some(OOMKilled)
        case r if r.contains("Evicted")                        => Some(Evicted)
        case r if r.contains("Error") || r.contains("BackOff") => Some(ContainerError)
        case _                                                 => None
      }
    }
  }

  private def containerLevelFailure(pod: PodStatusInfo): Option[FailureReason] = {
    val isDriver = pod.name.contains("driver")

    pod.containerStatuses.flatMap { container =>
      // Check for immediate failure conditions
      terminationFailure(container) orElse
        waitingStateFailure(container) orElse
        restartCountFailure(container, isDriver)
    }.headOption
  }

  private def terminationFailure(container: ContainerStatus): Option[FailureReason] = {
    if (container.state != "terminated") return None

    container.terminationReason.flatMap { reason =>
      reason match {
        case r if r.contains("OOMKilled")                             => Some(OOMKilled)
        case r if r.contains("Error")                                 => Some(ContainerError)
        case r if r.contains("ExitCode") && !r.contains("ExitCode:0") => Some(NonZeroExit)
        case _                                                        => None
      }
    }
  }

  private def waitingStateFailure(container: ContainerStatus): Option[FailureReason] = {
    if (container.state != "waiting") return None

    container.terminationReason.flatMap { reason =>
      reason match {
        case r if r.contains("CrashLoopBackOff")                        => Some(CrashLoopBackOff)
        case r if r.contains("ImagePull") || r.contains("ErrImagePull") => Some(ImagePullError)
        case r if r.contains("InvalidImageName")                        => Some(ImagePullError)
        case _                                                          => None
      }
    }
  }

  private def restartCountFailure(
      container: ContainerStatus,
      isDriver: Boolean
  ): Option[FailureReason] = {
    // Drivers should not restart, executors can restart once
    val maxRestarts = if (isDriver) 0 else TestConstants.ExecutorMaxRestarts

    if (container.state == "terminated" && container.restartCount > maxRestarts) {
      Some(ExcessiveRestarts)
    } else {
      None
    }
  }

  def failureMessage(reason: FailureReason): String = reason match {
    case PodFailed         => "Pod failed"
    case ContainerError    => "Container error"
    case OOMKilled         => "Out of memory"
    case ImagePullError    => "Image pull error"
    case CrashLoopBackOff  => "Container crash loop"
    case Evicted           => "Pod evicted"
    case NonZeroExit       => "Non-zero exit code"
    case ExcessiveRestarts => "Too many restarts"
  }
}
