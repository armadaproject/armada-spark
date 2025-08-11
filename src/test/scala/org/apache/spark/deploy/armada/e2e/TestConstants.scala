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

import scala.concurrent.duration._

/** Essential timeout and configuration values for E2E tests */
object TestConstants {

  val JobSubmitTimeout: FiniteDuration  = 60.seconds  // Time to submit job via Docker
  val JobWatchTimeout: FiniteDuration   = 300.seconds // Max time for job to complete (5 minutes)
  val PodRunningTimeout: FiniteDuration = 60.seconds  // Max time for pod to reach Running state
  val DefaultProcessTimeout: FiniteDuration = 10.seconds // Default timeout for CLI commands

  val PodCheckInterval: FiniteDuration = 5.seconds // How often to check for pod failures

  val DefaultMaxRetries                 = 3        // Number of retries for transient failures
  val DefaultRetryDelay: FiniteDuration = 1.second // Delay between retries

  val QueueCreationTimeout: FiniteDuration = 60.seconds // Max time for queue to become available

  val IngressCreationTimeout: FiniteDuration = 30.seconds // Max time to wait for ingress creation
  val AssertionThreadTimeout: FiniteDuration = 30.seconds // Max time to wait for assertion thread completion

  val MaxLogLines         = 200 // Max lines to capture from pod logs
  val MaxPreviousLogLines = 100 // Max lines from previous container logs
  val MaxEventLines       = 50  // Max event lines to capture

  val ClusterReadyTimeout: FiniteDuration     = 300.seconds
  val ClusterCheckRetryDelay: FiniteDuration  = 2.seconds
  val PodStatusUpdateInterval: FiniteDuration = 10.seconds
  val ProgressLogInterval: FiniteDuration     = 30.seconds
  val PodLogCaptureDelay                      = 500L // milliseconds
  val KubectlMaxRetries                       = 3
  val KubectlRetryDelay: FiniteDuration       = 1.second
}
