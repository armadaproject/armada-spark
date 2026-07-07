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

import api.event.{ContainerStatus, JobFailedEvent, JobPreemptedEvent}
import io.armadaproject.armada.ArmadaClient
import org.mockito.ArgumentMatchers.{anyInt, anyString}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class ArmadaEventWatcherSuite extends AnyFunSuite with Matchers with TableDrivenPropertyChecks {

  private val Queue    = "test-queue"
  private val JobSetId = "test-jobset"

  /** Cause is left at its protobuf default (`Cause.Error`), so `handleFailed` always appends ",
    * Cause: Error" to the event's `reason` when building `fullReason`.
    */
  private val CauseSuffix = ", Cause: Error"

  private def newWatcher(
      backend: ArmadaClusterManagerBackend,
      jobIdToExecutor: ConcurrentHashMap[String, String]
  ): ArmadaEventWatcher = {
    val armadaClient = mock(classOf[ArmadaClient])
    new ArmadaEventWatcher(Queue, JobSetId, backend, jobIdToExecutor, armadaClient)
  }

  /** A watcher wired to a fresh mock backend, with a single jobId -> executorId mapping registered
    * so handlers don't short-circuit on the unknown-job guard.
    */
  private def registeredWatcher(
      jobId: String,
      executorId: String
  ): (ArmadaEventWatcher, ArmadaClusterManagerBackend) = {
    val backend         = mock(classOf[ArmadaClusterManagerBackend])
    val jobIdToExecutor = new ConcurrentHashMap[String, String]()
    jobIdToExecutor.put(jobId, executorId)
    (newWatcher(backend, jobIdToExecutor), backend)
  }

  // Real Armada preemption-description templates (placeholders substituted with representative
  // values), one per preemption-cause variant referenced in the PreemptionReasonMarker comment.
  // See:
  //  - https://github.com/armadaproject/armada/blob/master/internal/scheduler/scheduling/preemption_description.go
  //  - https://github.com/armadaproject/armada/blob/master/internal/scheduler/scheduling/optimiser/gang_scheduler.go
  private val preemptionReasons = Table(
    ("variant", "reason"),
    ("urgency", "Preempted by scheduler using urgency preemption - preempting job job-111"),
    ("fair-share", "Preempted by scheduler using fair share preemption - preempting job job-222"),
    (
      "market-based",
      "Preempted by scheduler using market based preemption - current job has a bid of " +
        "1.500000 - preempting job job-333 has a bid of 2.750000"
    ),
    (
      "fairness-optimiser",
      "Preempted by scheduler using fairness optimiser - preempting job job-444"
    ),
    (
      "reschedule-failure",
      "Preempted by scheduler due to the job failing to reschedule - possibly node resource " +
        "changed causing this job to be unschedulable\nNode Summary:\nnode-1"
    )
  )

  test(
    "handleFailed routes every real Armada preemption description variant to onExecutorPreempted"
  ) {
    forAll(preemptionReasons) { (variant: String, reason: String) =>
      val jobId              = s"job-$variant"
      val executorId         = s"exec-$variant"
      val (watcher, backend) = registeredWatcher(jobId, executorId)

      watcher.handleFailed(JobFailedEvent(jobId = jobId, reason = reason))

      verify(backend).onExecutorPreempted(jobId, executorId, reason + CauseSuffix)
      verify(backend, never()).onExecutorFailed(anyString(), anyString(), anyInt(), anyString())
    }
  }

  private val crashReasons = Table(
    ("description", "reason", "exitCode"),
    ("OOMKilled", "OOMKilled", 137),
    ("non-zero exit", "Error: Container exited with a non-zero status of 1", 1)
  )

  test("handleFailed routes genuine crash reasons to onExecutorFailed, not onExecutorPreempted") {
    forAll(crashReasons) { (description: String, reason: String, exitCode: Int) =>
      val jobId              = s"job-${description.hashCode}"
      val executorId         = s"exec-${description.hashCode}"
      val (watcher, backend) = registeredWatcher(jobId, executorId)

      watcher.handleFailed(
        JobFailedEvent(
          jobId = jobId,
          reason = reason,
          containerStatuses = Seq(ContainerStatus(exitCode = exitCode))
        )
      )

      verify(backend).onExecutorFailed(jobId, executorId, exitCode, reason + CauseSuffix)
      verify(backend, never()).onExecutorPreempted(anyString(), anyString(), anyString())
    }
  }

  private val caseVariants = Table(
    "reason",
    "preempted by scheduler using urgency preemption - preempting job job-1",
    "PREEMPTED BY SCHEDULER USING URGENCY PREEMPTION - PREEMPTING JOB JOB-1"
  )

  test("handleFailed matches the preemption marker case-insensitively") {
    forAll(caseVariants) { reason: String =>
      val jobId              = s"job-${reason.hashCode}"
      val executorId         = s"exec-${reason.hashCode}"
      val (watcher, backend) = registeredWatcher(jobId, executorId)

      watcher.handleFailed(JobFailedEvent(jobId = jobId, reason = reason))

      verify(backend).onExecutorPreempted(jobId, executorId, reason + CauseSuffix)
      verify(backend, never()).onExecutorFailed(anyString(), anyString(), anyInt(), anyString())
    }
  }

  test("handlePreempted (typed JobPreemptedEvent) routes to onExecutorPreempted") {
    val jobId              = "job-typed-preempt"
    val executorId         = "exec-typed-preempt"
    val (watcher, backend) = registeredWatcher(jobId, executorId)

    watcher.handlePreempted(JobPreemptedEvent(jobId = jobId))

    verify(backend).onExecutorPreempted(jobId, executorId, "Preempted by Armada")
    verify(backend, never()).onExecutorFailed(anyString(), anyString(), anyInt(), anyString())
  }

  test("handleFailed for an unknown job id is a no-op") {
    val backend         = mock(classOf[ArmadaClusterManagerBackend])
    val jobIdToExecutor = new ConcurrentHashMap[String, String]()
    val watcher         = newWatcher(backend, jobIdToExecutor)

    watcher.handleFailed(
      JobFailedEvent(jobId = "unknown-job", reason = "Preempted by scheduler using urgency")
    )

    verify(backend, never()).onExecutorPreempted(anyString(), anyString(), anyString())
    verify(backend, never()).onExecutorFailed(anyString(), anyString(), anyInt(), anyString())
  }

  test("handlePreempted for an unknown job id is a no-op") {
    val backend         = mock(classOf[ArmadaClusterManagerBackend])
    val jobIdToExecutor = new ConcurrentHashMap[String, String]()
    val watcher         = newWatcher(backend, jobIdToExecutor)

    watcher.handlePreempted(JobPreemptedEvent(jobId = "unknown-job"))

    verify(backend, never()).onExecutorPreempted(anyString(), anyString(), anyString())
    verify(backend, never()).onExecutorFailed(anyString(), anyString(), anyInt(), anyString())
  }
}
