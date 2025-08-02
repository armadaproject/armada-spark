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

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.concurrent.duration._
import TestConstants._
import scala.util.{Failure, Success, Try}
import java.util.concurrent.atomic.AtomicInteger

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
  def ensureQueueExists(name: String)(implicit ec: ExecutionContext): Future[Unit]

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
  private val processTimeout = DefaultProcessTimeout
  private val retryConfig =
    RetryConfig(maxAttempts = DefaultMaxRetries, initialDelay = DefaultRetryDelay)

  private val yamlMapper = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  override def createQueue(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    Retry.withBackoff(retryConfig) {
      val cmd = buildCommand(s"create queue $name")
      ProcessExecutor.execute(cmd, processTimeout)
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
  override def ensureQueueExists(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    getQueue(name)
      .flatMap {
        case Some(_) =>
          println(s"[QUEUE] Queue $name already exists")
          Future.successful(())
        case None =>
          println(s"[QUEUE] Creating queue $name...")
          createQueue(name)
            .flatMap { _ =>
              println(s"[QUEUE] Polling for queue $name to become visible...")
              // Poll until queue is visible - we need multiple successful checks
              // to ensure the queue is stable and fully propagated
              val successCount      = new AtomicInteger(0)
              val requiredSuccesses = 3

              Retry.pollUntil(
                condition = {
                  val exists = getQueueSync(name).isDefined
                  if (exists) {
                    val count = successCount.incrementAndGet()
                    println(s"[QUEUE] Queue $name visible (check $count/$requiredSuccesses)")
                    if (count >= requiredSuccesses) {
                      true
                    } else {
                      // Continue checking even though it exists
                      Thread.sleep(2000) // Wait 2 seconds between successful checks
                      false
                    }
                  } else {
                    successCount.set(0) // Reset if we fail to see it
                    false
                  }
                },
                timeout = QueueCreationTimeout,
                pollInterval = QueuePollInterval
              )
            }
            .map { _ =>
              println(s"[QUEUE] Queue $name is ready")
              ()
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
    // Use slightly less than the provided timeout to allow for command overhead
    val watchTimeout = (timeout.toSeconds - 10).seconds.max(30.seconds)

    val watchFuture = Future {
      val cmd = buildCommand(s"watch $queue $jobSetId --exit-if-inactive")

      val startTime = System.currentTimeMillis()
      val handle    = ProcessExecutor.executeAsync(cmd)

      // Start a background thread for periodic updates with proper termination
      @volatile var shouldStop = false
      val updateThread = new Thread(() => {
        while (!shouldStop && handle.isAlive) {
          Thread.sleep(PodStatusUpdateInterval.toMillis)
          if (!shouldStop && handle.isAlive) {
            val elapsed = (System.currentTimeMillis() - startTime) / 1000
            // Log progress periodically for long-running jobs
            if (elapsed % ProgressLogInterval.toSeconds == 0 && elapsed > 0) {
              println(s"Still monitoring job - elapsed: ${elapsed}s")
            }
          }
        }
      })
      updateThread.setDaemon(true)
      updateThread.start()

      try {
        val result  = handle.waitFor(watchTimeout)(ec)
        val elapsed = (System.currentTimeMillis() - startTime) / 1000

        if (result.timedOut) {
          None // Watch timed out
        } else if (result.exitCode == 0) {
          Some(JobSetStatus.Success)
        } else {
          Some(JobSetStatus.Failed)
        }
      } finally {
        shouldStop = true
        updateThread.interrupt() // Ensure thread stops
      }
    }

    watchFuture.map {
      case Some(status) =>
        status
      case None =>
        // If watch times out, return Timeout status
        JobSetStatus.Timeout
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
    } match {
      case Success(queue) => Some(queue)
      case Failure(ex)    =>
        // Only log parse errors, not "queue not found" errors
        if (!ex.getMessage.contains("exit code")) {
          println(s"[ERROR] Failed to parse queue YAML for $name: ${ex.getMessage}")
        }
        None
    }
  }

  /** Builds armadactl command with proper arguments. Splits subCommand by spaces and appends
    * armadaUrl flag. Tries to find armadactl in PATH or uses system property.
    */
  private def buildCommand(subCommand: String): Seq[String] = {
    val armadactlCmd = resolveArmadactlPath.getOrElse {
      throw new RuntimeException("armadactl not found in system properties or PATH")
    }
    Seq(armadactlCmd) ++ subCommand.split(" ") ++ Seq("--armadaUrl", armadaUrl)
  }

  /** Resolves the path to `armadactl`:
    *   - Uses `armadactl.path` system property if set.
    *   - Otherwise searches for `armadactl` in `$PATH`.
    *
    * @return
    *   Some(path) if found, None otherwise.
    */
  private def resolveArmadactlPath: Option[String] = {
    // First check system property
    sys.props.get("armadactl.path") match {
      case Some(path) =>
        Some(path)
      case None =>
        // Fall back to PATH search
        val pathSep = java.io.File.pathSeparator
        val pathEnv = sys.env.get("PATH").getOrElse("")

        val pathDirs = pathEnv.split(pathSep).filter(_.nonEmpty)
        val found = pathDirs
          .map(dir => new java.io.File(dir, "armadactl"))
          .find(_.exists())
          .map(_.getAbsolutePath)

        found match {
          case Some(path) =>
            Some(path)
          case None =>
            None
        }
    }
  }
}
