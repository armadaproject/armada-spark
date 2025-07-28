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

import api.submit.Queue
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Abstraction for Armada operations used in E2E testing.
  *
  * Provides async methods for queue management and job monitoring. All operations are non-blocking
  * and return Futures for composition.
  */
trait ArmadaOperations {

  /** Creates a new queue in Armada.
    * @param name
    *   Queue name to create
    */
  def createQueue(name: String)(implicit ec: ExecutionContext): Future[Unit]

  /** Retrieves queue information if it exists.
    * @param name
    *   Queue name to query
    * @return
    *   Future containing the queue if found, None otherwise
    */
  def getQueue(name: String)(implicit ec: ExecutionContext): Future[Option[Queue]]

  /** Ensures a queue exists, creating it if necessary. Idempotent operation that succeeds if queue
    * exists after call.
    * @param name
    *   Queue name to ensure exists
    */
  def ensureQueue(name: String)(implicit ec: ExecutionContext): Future[Unit]

  /** Deletes a queue if supported by Armada.
    * @param name
    *   Queue name to delete
    */
  def deleteQueue(name: String)(implicit ec: ExecutionContext): Future[Unit]

  /** Watches a job set until completion or timeout. Uses armadactl watch with --exit-if-inactive
    * flag.
    * @param queue
    *   Queue containing the job set
    * @param jobSetId
    *   Job set identifier to watch
    * @param timeout
    *   Maximum time to wait for completion
    * @return
    *   Future containing final job status (Success, Failed, or Timeout)
    */
  def watchJobSet(queue: String, jobSetId: String, timeout: Duration)(implicit
      ec: ExecutionContext
  ): Future[JobSetStatus]
}

sealed trait JobSetStatus
object JobSetStatus {
  case object Success extends JobSetStatus
  case object Failed  extends JobSetStatus
  case object Timeout extends JobSetStatus
}

/** Armada client implementation using armadactl CLI.
  *
  *   - Uses armadactl commands executed via ProcessExecutor
  *   - Parses YAML output using Jackson
  *   - Implements retry logic with exponential backoff for transient failures
  *   - All operations are async and non-blocking
  *   - Queue creation is idempotent (doesn't fail if queue exists)
  *
  * @param armadaUrl
  *   Armada server URL (default: "localhost:30002")
  */
class ArmadaClient(armadaUrl: String = "localhost:30002") extends ArmadaOperations {
  private val processTimeout = 30.seconds
  private val retryConfig    = RetryConfig(maxAttempts = 3, initialDelay = 1.second)

  private val yamlMapper = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  override def createQueue(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    Retry.withBackoff(retryConfig) {
      ProcessExecutor.execute(
        buildCommand(s"create queue $name"),
        processTimeout
      )
      ()
    }
  }

  override def getQueue(name: String)(implicit ec: ExecutionContext): Future[Option[Queue]] = {
    Retry
      .withBackoff(retryConfig) {
        val result = ProcessExecutor.execute(
          buildCommand(s"get queue $name"),
          processTimeout
        )
        Some(yamlMapper.readValue(result.stdout, classOf[Queue]))
      }
      .recover { case _ =>
        None
      }
  }

  /** Ensures queue exists by checking first, creating if needed, then polling until ready. This
    * handles eventual consistency in distributed systems.
    */
  override def ensureQueue(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    getQueue(name).flatMap {
      case Some(_) => Future.successful(())
      case None =>
        createQueue(name).flatMap { _ =>
          // Poll until queue is visible (handles eventual consistency)
          Retry.pollUntil(
            condition = getQueueSync(name).isDefined,
            timeout = 30.seconds,
            pollInterval = 2.seconds
          )
        }
    }
  }

  /** Watches job set using armadactl watch command. --exit-if-inactive flag ensures watch exits
    * when job completes. Exit code 0 = success, non-zero = failed job.
    */
  override def deleteQueue(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      Try(
        ProcessExecutor.execute(
          buildCommand(s"delete queue $name"),
          processTimeout
        )
      )
      ()
    }.recover { case _ =>
      () // Queue might not exist
    }
  }

  override def watchJobSet(
      queue: String,
      jobSetId: String,
      timeout: Duration
  )(implicit ec: ExecutionContext): Future[JobSetStatus] = {
    Future {
      val handle = ProcessExecutor.executeAsync(
        buildCommand(s"watch $queue $jobSetId --exit-if-inactive")
      )

      val result = handle.waitFor(timeout)(ec)

      if (result.timedOut) {
        JobSetStatus.Timeout
      } else if (result.exitCode == 0) {
        JobSetStatus.Success
      } else {
        JobSetStatus.Failed
      }
    }
  }

  /** Synchronous queue check used for polling in ensureQueue. Returns None on any failure (queue
    * doesn't exist or network error).
    */
  private def getQueueSync(name: String): Option[Queue] = {
    Try {
      val result = ProcessExecutor.execute(
        buildCommand(s"get queue $name"),
        processTimeout
      )
      yamlMapper.readValue(result.stdout, classOf[Queue])
    }.toOption
  }

  /** Builds armadactl command with proper arguments. Splits subCommand by spaces and appends
    * armadaUrl flag.
    */
  private def buildCommand(subCommand: String): Seq[String] = {
    Seq("armadactl") ++ subCommand.split(" ") ++ Seq("--armadaUrl", armadaUrl)
  }
}
