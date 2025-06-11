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

import scala.concurrent.duration._
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/** Wrapper class for armadactl CLI operations to simplify e2e testing. Provides convenient methods
  * for queue management and job monitoring.
  *
  * @param armadaUrl
  *   The Armada server URL (defaults to localhost:30002)
  */
class ArmadaCtlWrapper(val armadaUrl: String = "localhost:30002") {
  private val yamlMapper = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    // Configure Jackson to ignore unknown properties like apiVersion, kind
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  /** Create a queue with the specified name.
    *
    * @param queueName
    *   The name of the queue to create
    * @return
    *   Success with output or Failure with exception
    */
  def createQueue(queueName: String): Try[String] = {
    Try(buildCommand(s"create queue $queueName").!!)
  }

  /** Delete a queue with the specified name.
    *
    * @param queueName
    *   The name of the queue to delete
    * @return
    *   Success with output or Failure with exception
    */
  def deleteQueue(queueName: String): Try[String] = {
    Try(buildCommand(s"delete queue $queueName").!!)
  }

  /** Get information about a specific queue.
    *
    * @param queueName
    *   The name of the queue to query
    * @return
    *   Success with parsed Queue object or Failure with exception
    */
  def getQueue(queueName: String): Try[Queue] = {
    Try(buildCommand(s"get queue $queueName").!!).flatMap(parseQueueYaml)
  }

  /** Get raw YAML information about a specific queue.
    *
    * @param queueName
    *   The name of the queue to query
    * @return
    *   Success with queue info as string or Failure with exception
    */
  def getQueueRaw(queueName: String): Try[String] = {
    Try(buildCommand(s"get queue $queueName").!!)
  }

  /** List all queues.
    *
    * @return
    *   Success with all queues info or Failure with exception
    */
  def listQueues(): Try[String] = {
    Try(buildCommand("get queues").!!)
  }

  /** Check if a queue exists.
    *
    * @param queueName
    *   The name of the queue to check
    * @return
    *   true if queue exists, false otherwise
    */
  def queueExists(queueName: String): Boolean = {
    getQueue(queueName).isSuccess
  }

  /** Parse YAML output directly to Queue object.
    *
    * @param yamlOutput
    *   The YAML string from armadactl
    * @return
    *   Parsed Queue object or Failure with parse exception
    */
  private[e2e] def parseQueueYaml(yamlOutput: String): Try[Queue] = {
    Try {
      yamlMapper.readValue(yamlOutput, classOf[Queue])
    }
  }

  /** Verify queue exists with retry logic for eventual consistency.
    *
    * @param queueName
    *   The name of the queue to verify
    * @param maxAttempts
    *   Maximum number of retry attempts
    * @param retryDelay
    *   Delay between retry attempts
    * @return
    *   true if queue exists within retry limits, false otherwise
    */
  def verifyQueueExists(
      queueName: String,
      maxAttempts: Int = 5,
      retryDelay: Duration = 2.seconds
  ): Boolean = {
    0 until maxAttempts foreach { attempt =>
      getQueue(queueName) match {
        case Success(_) =>
          return true
        case Failure(_) =>
          if (attempt < maxAttempts - 1) {
            Thread.sleep(retryDelay.toMillis)
          }
      }
    }
    false
  }

  /** Watch a jobset for completion using armadactl watch with --exit-if-inactive.
    *
    * @param queueName
    *   The queue containing the jobset
    * @param jobSetId
    *   The jobset ID to watch
    * @param timeout
    *   Maximum time to wait for completion
    * @return
    *   JobSetResult indicating success, failure, or timeout
    */
  def watchJobSet(
      queueName: String,
      jobSetId: String,
      timeout: Duration
  ): JobSetResult.Value = {
    Try {
      val process = buildCommand(s"watch $queueName $jobSetId --exit-if-inactive")

      val future = scala.concurrent.Future {
        process.run().exitValue()
      }(scala.concurrent.ExecutionContext.global)

      Try {
        scala.concurrent.Await.result(future, timeout)
      } match {
        case Success(exitCode) =>
          exitCode match {
            case 0 => JobSetResult.Success
            case _ => JobSetResult.Failed
          }
        case Failure(_) =>
          JobSetResult.Timeout
      }
    } match {
      case Success(result) => result
      case Failure(_)      => JobSetResult.Failed
    }
  }

  /** Create a queue if it doesn't already exist.
    *
    * @param queueName
    *   The name of the queue to ensure exists
    * @return
    *   Success with creation/existence message or Failure with exception
    */
  def ensureQueueExists(queueName: String): Try[String] = {
    if (queueExists(queueName)) {
      Success(s"Queue $queueName already exists")
    } else {
      createQueue(queueName)
    }
  }

  /** Build an armadactl command as a ProcessBuilder.
    *
    * @param subCommand
    *   The armadactl subcommand and arguments (without armadactl prefix)
    * @return
    *   ProcessBuilder object for command execution
    */
  private def buildCommand(subCommand: String): ProcessBuilder = {
    val command = s"armadactl $subCommand --armadaUrl $armadaUrl"
    Process(command)
  }
}

/** Enum for jobset completion results from armadactl watch.
  */
object JobSetResult extends Enumeration {
  type JobSetResult = Value
  val Success, Failed, Timeout = Value
}
