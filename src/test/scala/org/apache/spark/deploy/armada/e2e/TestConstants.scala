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

/** Constants used throughout the E2E test framework */
object TestConstants {

  // Timing constants
  val PodCheckInterval             = 3.seconds
  val PodStatusUpdateInterval      = 5.seconds
  val ProgressLogInterval          = 30.seconds
  val IngressCreationCheckInterval = 2.seconds
  val QueuePollInterval            = 2.seconds
  val QueuePropagationDelay        = 5.seconds
  val ClusterCheckRetryDelay       = 10.seconds

  // Timeout constants
  val DefaultProcessTimeout  = 30.seconds
  val JobSubmitTimeout       = 30.seconds
  val JobWatchTimeout        = 300.seconds
  val QueueCreationTimeout   = 30.seconds
  val PodRunningTimeout      = 240.seconds
  val IngressCreationTimeout = 30.seconds
  val ClusterReadyTimeout    = 300.seconds

  // Retry constants
  val DefaultMaxRetries = 3
  val DefaultRetryDelay = 1.second
  val KubectlMaxRetries = 3
  val KubectlRetryDelay = 1.second

  // Resource limits
  val MaxLogLines         = 200
  val MaxPreviousLogLines = 100
  val MaxEventLines       = 30
  val MaxErrorLines       = 10

  // Thread management
  val ThreadJoinTimeout       = 1000L // milliseconds
  val ThreadSleepAfterDestroy = 100L  // milliseconds
  val PodLogCaptureDelay      = 500L  // milliseconds

  // Executor failure thresholds
  val ExecutorMaxRestarts = 1
}
