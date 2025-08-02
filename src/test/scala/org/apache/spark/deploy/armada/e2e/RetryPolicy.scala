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

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.annotation.tailrec
import java.util.concurrent.TimeoutException

case class RetryConfig(
    maxAttempts: Int = 5,
    initialDelay: Duration = 1.second,
    maxDelay: Duration = 30.seconds,
    backoffMultiplier: Double = 2.0,
    jitter: Double = 0.1
) {
  require(maxAttempts > 0)
  require(initialDelay > Duration.Zero)
  require(backoffMultiplier >= 1.0)
  require(jitter >= 0 && jitter <= 1.0)
}

object Retry {
  def withBackoff[T](config: RetryConfig = RetryConfig())(
      operation: => T
  )(implicit ec: ExecutionContext): Future[T] = {
    attemptWithBackoff(operation, config, 1)
  }

  def withBackoffSync[T](config: RetryConfig = RetryConfig())(operation: => T): T = {
    attemptWithBackoffSync(operation, config, 1)
  }

  private def attemptWithBackoff[T](
      operation: => T,
      config: RetryConfig,
      attempt: Int
  )(implicit ec: ExecutionContext): Future[T] = {
    Future(operation).recoverWith {
      case error if attempt < config.maxAttempts =>
        val delay = calculateDelay(config, attempt)
        Future {
          Thread.sleep(delay.toMillis)
        }.flatMap(_ => attemptWithBackoff(operation, config, attempt + 1))
      case finalError =>
        Future.failed(finalError)
    }
  }

  @tailrec
  private def attemptWithBackoffSync[T](
      operation: => T,
      config: RetryConfig,
      attempt: Int
  ): T = {
    Try(operation) match {
      case Success(result) => result
      case Failure(_) if attempt < config.maxAttempts =>
        val delay = calculateDelay(config, attempt)
        Thread.sleep(delay.toMillis)
        attemptWithBackoffSync(operation, config, attempt + 1)
      case Failure(ex) =>
        throw ex
    }
  }

  private def calculateDelay(config: RetryConfig, attempt: Int): Duration = {
    val baseDelay    = config.initialDelay * math.pow(config.backoffMultiplier, attempt - 1).toLong
    val clampedDelay = baseDelay.min(config.maxDelay)
    val jitterAmount = (clampedDelay.toMillis * config.jitter * math.random()).toLong
    clampedDelay + jitterAmount.millis
  }

  def pollUntil(
      condition: => Boolean,
      timeout: Duration,
      pollInterval: Duration = 1.second
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val startTime     = System.currentTimeMillis()
    val timeoutMillis = timeout.toMillis
    val promise       = Promise[Unit]()

    def checkCondition(): Unit = {
      if (condition) {
        promise.success(())
      } else if ((System.currentTimeMillis() - startTime) > timeoutMillis) {
        promise.failure(new TimeoutException(s"Condition not met within $timeout"))
      } else {
        ec.execute(() => {
          Thread.sleep(pollInterval.toMillis)
          checkCondition()
        })
      }
    }

    Future(checkCondition())
    promise.future
  }
}
