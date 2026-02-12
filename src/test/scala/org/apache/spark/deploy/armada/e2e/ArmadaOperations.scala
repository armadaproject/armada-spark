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
import TestConstants._
import scala.util.{Failure, Success, Try}

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
class ArmadaClient(armadaUrl: String = "localhost:30002") {
  private val processTimeout = DefaultProcessTimeout

  private val yamlMapper = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  def createQueue(name: String)(implicit ec: ExecutionContext): Future[Unit] = Future {
    var attempts                     = 0
    var success                      = false
    var lastError: Option[Exception] = None

    while (!success && attempts < 3) {
      try {
        val cmd = buildCommand(s"create queue $name")
        ProcessExecutor.execute(cmd, processTimeout)
        success = true
      } catch {
        case ex: Exception =>
          lastError = Some(ex)
          attempts += 1
          if (attempts < 3) Thread.sleep(1000)
      }
    }

    if (!success) {
      throw lastError.getOrElse(new RuntimeException(s"Failed to create queue $name"))
    }
  }

  def getQueue(name: String)(implicit ec: ExecutionContext): Future[Option[Queue]] = Future {
    try {
      val result = ProcessExecutor.execute(
        buildCommand(s"get queue $name"),
        processTimeout
      )
      Some(yamlMapper.readValue(result.stdout, classOf[Queue]))
    } catch {
      case _: Exception => None
    }
  }

  /** Ensures queue exists by checking first, creating if needed, then polling until ready. */
  def ensureQueueExists(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    getQueue(name).flatMap {
      case Some(_) =>
        println(s"[QUEUE] Queue $name already exists")
        Future.successful(())
      case None =>
        println(s"[QUEUE] Creating queue $name...")
        createQueue(name).flatMap { _ =>
          println(s"[QUEUE] Waiting for queue $name to become available...")
          Future {
            var attempts    = 0
            val maxAttempts = 30
            var queueFound  = false

            while (!queueFound && attempts < maxAttempts) {
              Thread.sleep(1000)
              queueFound = getQueueSync(name).isDefined
              attempts += 1
              if (!queueFound && attempts % 5 == 0) {
                println(s"[QUEUE] Still waiting for queue $name... (${attempts}s)")
              }
            }

            if (queueFound) {
              println(s"[QUEUE] Queue $name is ready")
            } else {
              throw new RuntimeException(s"Queue $name not available after $attempts seconds")
            }
          }
        }
    }
  }

  def deleteQueue(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
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

  /** Watches job set using armadactl watch command. --exit-if-inactive flag ensures watch exits
    * when job completes. Exit code 0 = success, non-zero = failed job.
    */
  def watchJobSet(
      queue: String,
      jobSetId: String,
      timeout: Duration
  )(implicit ec: ExecutionContext): Future[JobSetStatus] = {
    // Use slightly less than the provided timeout to allow for command overhead
    val watchTimeout = (timeout.toSeconds - 10).seconds.max(30.seconds)

    Future {
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
        val result = handle.waitFor(watchTimeout)(ec)

        if (result.timedOut) {
          JobSetStatus.Timeout
        } else if (result.exitCode == 0) {
          JobSetStatus.Success
        } else {
          JobSetStatus.Failed
        }
      } finally {
        shouldStop = true
        updateThread.interrupt() // Ensure thread stops
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
    } match {
      case Success(queue) => Some(queue)
      case Failure(_) =>
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
    // armadactl command expects the server address to be of the form
    // <hostname-or-IP>:<port> with no pseudo-protocol prefix
    // val input = "local://armada://localhost:30002"
    val pattern      = """.*armada://(.+)""".r
    var armadactlUrl = "undefined-armadactl-url"

    armadaUrl match {
      case pattern(hostPort) => armadactlUrl = hostPort // e.g. "localhost:30002"
      case _ =>
        throw new RuntimeException(
          s"could not extract valid armadactl URL from armada URL ${armadaUrl}"
        )
    }

    // var armadactlUrl = armadaUrl.replaceFirst("^armada://", "")
    println(s"-------- buildCommand():  armadaUrl = ${armadaUrl}")
    println(s"-------- buildCommand():  armadactlUrl = ${armadactlUrl}")
    Seq(armadactlCmd) ++ subCommand.split(" ") ++ Seq("--armadaUrl", armadactlUrl)
  }

  /** Resolves the path to `armadactl`:
    *   - Uses `armadactl.path` system property if set.
    *   - Otherwise searches for `armadactl` in `$PATH`.
    *
    * @return
    *   Some(path) if found, None otherwise.
    */
  private def resolveArmadactlPath: Option[String] = {
    sys.props.get("armadactl.path").orElse {
      val pathSep = java.io.File.pathSeparator
      val pathEnv = sys.env.getOrElse("PATH", "")

      pathEnv
        .split(pathSep)
        .filter(_.nonEmpty)
        .map(dir => new java.io.File(dir, "armadactl"))
        .find(_.exists())
        .map(_.getAbsolutePath)
    }
  }
}
