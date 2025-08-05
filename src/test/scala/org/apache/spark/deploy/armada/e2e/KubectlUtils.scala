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
import TestConstants._
import scala.util.{Try, Success, Failure}
import java.util.concurrent.TimeoutException

/** Utilities for executing kubectl commands with retry logic to handle transient failures */
object KubectlUtils {

  /** Execute kubectl command with automatic retry for transient failures
    * @param cmd
    *   The kubectl command to execute
    * @param timeout
    *   Timeout for each attempt
    * @param maxRetries
    *   Maximum number of retry attempts (default: 3)
    * @return
    *   ProcessResult from successful execution
    */
  def executeWithRetry(
      cmd: Seq[String],
      timeout: Duration = 10.seconds,
      maxRetries: Int = KubectlMaxRetries
  ): ProcessResult = {
    require(maxRetries > 0, "maxRetries must be positive")

    def isRetriable(result: ProcessResult): Boolean = {
      result.timedOut ||
      result.stderr.contains("connection refused") ||
      result.stderr.contains("Unable to connect") ||
      result.stderr.contains("timeout") ||
      result.stderr.contains("error dialing backend")
    }

    def attempt(retriesLeft: Int): ProcessResult = {
      Try(ProcessExecutor.execute(cmd, timeout)) match {
        case Success(result) if result.exitCode == 0 =>
          result
        case Success(result) if retriesLeft > 0 && isRetriable(result) =>
          Thread.sleep(KubectlRetryDelay.toMillis)
          attempt(retriesLeft - 1)
        case Success(result) =>
          result // Non-retriable failure or no retries left
        case Failure(ex: TimeoutException) if retriesLeft > 0 =>
          Thread.sleep(KubectlRetryDelay.toMillis)
          attempt(retriesLeft - 1)
        case Failure(ex) =>
          throw ex
      }
    }

    attempt(maxRetries - 1)
  }

}
