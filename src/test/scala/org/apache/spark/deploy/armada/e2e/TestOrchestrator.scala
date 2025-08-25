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

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import TestConstants._

import scala.annotation.tailrec

case class TestConfig(
    baseQueueName: String,
    imageName: String,
    masterUrl: String,
    lookoutUrl: String,
    scalaVersion: String,
    sparkVersion: String,
    sparkConfs: Map[String, String] = Map.empty,
    assertions: Seq[TestAssertion] = Seq.empty,
    failFastOnPodFailure: Boolean = true,
    pythonScript: Option[String] = None
)

/** Manages test isolation with unique namespace and queue per test. Ensures cleanup of resources
  * after test completion.
  */
case class TestContext(
    testName: String,
    testId: String = UUID.randomUUID().toString.take(8),
    startTime: Long = System.currentTimeMillis()
) {
  val namespace: String   = s"e2e-test-$testId"
  val queueSuffix: String = testId

  def labels: Map[String, String] = Map(
    "test-id"        -> testId,
    "test-name"      -> testName,
    "test-framework" -> "armada-spark-e2e"
  )
}

case class TestResult(
    jobSetId: String,
    queueName: String,
    status: JobSetStatus,
    assertionResults: Map[String, AssertionResult] = Map.empty
)

/** Orchestrates end-to-end testing of Spark jobs on Armada. */
class TestOrchestrator(
    armadaClient: ArmadaClient,
    k8sClient: K8sClient
)(implicit ec: ExecutionContext) {

  private val jobSubmitTimeout = JobSubmitTimeout
  private val jobWatchTimeout  = JobWatchTimeout

  private def runAssertionsWhileJobRunning(
      assertions: Seq[TestAssertion],
      context: TestContext,
      jobCompleted: () => Boolean
  ): Map[String, AssertionResult] = {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val assertionContext = AssertionContext(
      context.testId,
      context.namespace,
      context.testId,
      k8sClient,
      armadaClient
    )

    // Wait for Spark driver pod to be scheduled and running
    val driverSelector  = s"spark-role=driver,test-id=${context.testId}"
    var driverRunning   = false
    var attempts        = 0
    val maxWaitAttempts = 60 // 2 minutes max to wait for driver

    while (!driverRunning && !jobCompleted() && attempts < maxWaitAttempts) {
      try {
        val driverPod = Await.result(
          k8sClient.getPodByLabel(driverSelector, context.namespace),
          10.seconds
        )
        driverPod match {
          case Some(pod) if pod.status.phase == "Running" =>
            driverRunning = true
          case Some(_) =>
          case None    =>
        }
      } catch {
        case _: Exception =>
      }

      if (!driverRunning) {
        Thread.sleep(2000)
        attempts += 1
      }
    }

    if (!driverRunning) {
      return assertions.map { assertion =>
        assertion.name -> AssertionResult.Failure(
          "Driver pod never started, Armada may not have scheduled it",
          None
        )
      }.toMap
    }

    assertions.map { assertion =>
      val result = validateScheduling(assertion, assertionContext, jobCompleted)
      assertion.name -> result
    }.toMap
  }

  private def validateScheduling(
      assertion: TestAssertion,
      context: AssertionContext,
      jobCompleted: () => Boolean
  ): AssertionResult = {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    // Give Armada some time to schedule resources after driver starts (executors need time to come up)
    val maxWaitTime                 = 30.seconds
    val startTime                   = System.currentTimeMillis()
    var attempts                    = 0
    var lastResult: AssertionResult = AssertionResult.Failure("Not yet checked", None)

    while (!jobCompleted() && (System.currentTimeMillis() - startTime) < maxWaitTime.toMillis) {
      attempts += 1
      try {
        val result = Await.result(assertion.assert(context)(ec), 10.seconds)
        lastResult = result

        result match {
          case AssertionResult.Success =>
            return result
          case AssertionResult.Failure(_, _) =>
          // For the first few attempts, be patient - resources may still be scheduling
        }
      } catch {
        case ex: Exception =>
          lastResult = AssertionResult.Failure(ex.getMessage, Some(ex))
        // Don't log retries to avoid clutter
      }

      Thread.sleep(2000)
    }

    lastResult match {
      case AssertionResult.Success =>
        lastResult
      case AssertionResult.Failure(_, cause) =>
        if (jobCompleted()) {
          AssertionResult.Failure(
            s"${assertion.name}: Job completed before assertion could pass",
            cause
          )
        } else {
          lastResult
        }
    }
  }

  def runTest(name: String, config: TestConfig): Future[TestResult] = {
    val context = TestContext(name)
    val testJobSetId =
      s"e2e-${name.toLowerCase.replaceAll("[^a-z0-9]", "-")}-${System.currentTimeMillis()}"
    val queueName = s"${config.baseQueueName}-${context.queueSuffix}"

    println(s"\n========== Starting E2E Test: $name ==========")
    println(s"Test ID: ${context.testId}")
    println(s"Namespace: ${context.namespace}")
    println(s"Queue: $queueName")

    val resultFuture = for {
      _ <- k8sClient.createNamespace(context.namespace)
      _ <- armadaClient.ensureQueueExists(queueName).recoverWith { case ex =>
        throw new RuntimeException(s"Failed to ensure queue $queueName", ex)
      }
      _      <- submitJob(testJobSetId, queueName, name, config, context)
      result <- watchJob(queueName, testJobSetId, config, context)
    } yield result

    resultFuture
      .andThen {
        case scala.util.Failure(ex) =>
          // Capture debug info on any failure
          println(s"\n[FAILURE] Test failed with exception: ${ex.getMessage}")
          val podMonitor = new SimplePodMonitor(context.namespace)
          podMonitor.captureDebugInfo()
          podMonitor.printCapturedLogs()
        case _ =>
      }
      .andThen { case _ =>
        cleanupTest(context, queueName)
      }
      .map { result =>
        println(s"\n========== Test Finished: $name ==========")
        println(s"Job Status: ${result.status}")
        println(s"Duration: ${(System.currentTimeMillis() - context.startTime) / 1000}s")
        result
      }
  }

  private def cleanupTest(context: TestContext, queueName: String): Future[Unit] = {
    for {
      _ <- k8sClient.deleteNamespace(context.namespace).recover { case ex =>
        println(
          s"[CLEANUP] Warning: Failed to delete namespace ${context.namespace}: ${ex.getMessage}"
        )
        ()
      }
      _ <- armadaClient.deleteQueue(queueName).recover { case ex =>
        println(s"[CLEANUP] Warning: Failed to delete queue $queueName: ${ex.getMessage}")
        ()
      }
    } yield ()
  }

  private def submitJob(
      jobSetId: String,
      queueName: String,
      testName: String,
      config: TestConfig,
      context: TestContext
  ): Future[Unit] = Future {
    // Use spark-examples JAR with the correct path based on Scala binary version and Spark version
    // Following the same pattern as scripts/init.sh
    val appResource = config.pythonScript.getOrElse(
      s"local:///opt/spark/examples/jars/spark-examples_${config.scalaVersion}-${config.sparkVersion}.jar"
    )
    val volumeMounts = buildVolumeMounts()

    val contextLabelString = context.labels.iterator.map { case (k, v) => s"$k=$v" }.mkString(",")
    val mergedLabels = config.sparkConfs
      .get("spark.armada.pod.labels")
      .map(existing => s"$existing,$contextLabelString")
      .getOrElse(contextLabelString)

    val enhancedSparkConfs = config.sparkConfs ++ Map(
      "spark.armada.pod.labels"           -> mergedLabels,
      "spark.armada.scheduling.namespace" -> context.namespace
    )

    val dockerCommand = buildDockerCommand(
      config.imageName,
      volumeMounts,
      config.masterUrl,
      testName,
      queueName,
      jobSetId,
      enhancedSparkConfs,
      appResource,
      config.lookoutUrl,
      config.pythonScript
    )

    println(s"\n[SUBMIT] Submitting Spark job via Docker:")
    println(s"[SUBMIT]   Queue: $queueName")
    println(s"[SUBMIT]   JobSetId: $jobSetId")
    println(s"[SUBMIT]   Namespace: ${context.namespace}")
    println(s"[SUBMIT]   Image: ${config.imageName}")
    println(s"[SUBMIT]   Master URL: ${config.masterUrl}")
    println(s"[SUBMIT]   Application Resource: $appResource")
    println(s"[SUBMIT]   Spark config:")
    enhancedSparkConfs.toSeq.sortBy(_._1).foreach { case (key, value) =>
      val displayValue = if (value.length > 100) value.take(100) + "..." else value
      println(s"[SUBMIT]     $key = $displayValue")
    }
    // Properly escape command for shell reproduction
    val escapedCommand = dockerCommand.map { arg =>
      if (arg.contains(" ") || arg.contains("'") || arg.contains("\"")) {
        "'" + arg.replace("'", "'\\''") + "'"
      } else arg
    }
    println(s"[SUBMIT] Full command: ${escapedCommand.mkString(" ")}\n")

    @tailrec
    def attemptSubmit(attempt: Int = 1): ProcessResult = {
      val result = ProcessExecutor.executeWithResult(dockerCommand, jobSubmitTimeout)

      if (result.exitCode != 0) {
        val allOutput            = result.stdout + "\n" + result.stderr
        val isQueueNotFoundError = allOutput.contains("PERMISSION_DENIED: could not find queue")

        if (isQueueNotFoundError && attempt <= 3) {
          val waitTime = attempt * 2 // 2, 4, 6 seconds
          println(
            s"[SUBMIT] Queue not found error on attempt $attempt, retrying in ${waitTime}s..."
          )
          Thread.sleep(waitTime * 1000)
          attemptSubmit(attempt + 1)
        } else {
          // Try to find the actual error in stderr or stdout
          val errorPattern = """Exception|Error|Failed|FAILED""".r
          val relevantLines = allOutput
            .split("\n")
            .filter(line => errorPattern.findFirstIn(line).isDefined)
            .take(10)

          println(
            s"[SUBMIT] ERROR Submit failed with exit code ${result.exitCode} after $attempt attempts"
          )
          if (relevantLines.nonEmpty) {
            println("[SUBMIT] Relevant error lines:")
            relevantLines.foreach(line => println(s"[SUBMIT]   $line"))
          }

          throw new RuntimeException(s"Spark submit failed with exit code ${result.exitCode}")
        }
      } else {
        if (attempt > 1) {
          println(s"[SUBMIT] Job submitted successfully on attempt $attempt")
        } else {
          println(s"[SUBMIT] Job submitted successfully")
        }
        result
      }
    }

    attemptSubmit()
  }

  private def watchJob(
      queueName: String,
      jobSetId: String,
      config: TestConfig,
      context: TestContext
  ): Future[TestResult] = Future {
    val podMonitor = new SimplePodMonitor(context.namespace)

    println(s"[WATCH] Starting job watch for jobSetId: $jobSetId")
    val jobFuture = armadaClient.watchJobSet(queueName, jobSetId, jobWatchTimeout)

    var jobCompleted               = false
    var podFailure: Option[String] = None
    var assertionResults           = Map.empty[String, AssertionResult]

    if (config.failFastOnPodFailure) {
      println(s"[MONITOR] Starting pod monitoring for namespace: ${context.namespace}")
      val monitorThread = new Thread(() => {
        while (!jobCompleted && podFailure.isEmpty) {
          podFailure = podMonitor.checkForFailures()
          if (podFailure.isEmpty) {
            Thread.sleep(5000) // Check every 5 seconds
          }
        }
      })
      monitorThread.setDaemon(true)
      monitorThread.start()
    }

    // If we have assertions, run them after driver starts but while job is running
    val assertionThread = if (config.assertions.nonEmpty) {
      val thread = new Thread(() => {
        // Wait for driver to start, then run assertions while job is active
        assertionResults = runAssertionsWhileJobRunning(
          config.assertions,
          context,
          jobCompleted = () => jobCompleted
        )
      })
      thread.setDaemon(true)
      thread.start()
      Some(thread)
    } else {
      None
    }

    // Wait for job to complete or fail
    val jobStatus =
      try {
        import scala.concurrent.Await
        Await.result(jobFuture, jobWatchTimeout + 10.seconds)
      } catch {
        case _: TimeoutException =>
          println(s"[TIMEOUT] Job watch timed out after ${jobWatchTimeout.toSeconds}s")
          JobSetStatus.Timeout
        case ex: Exception =>
          println(s"[ERROR] Job watch failed: ${ex.getMessage}")
          JobSetStatus.Failed
      } finally {
        jobCompleted = true
      }

    // Wait for assertion thread to complete if it exists
    val assertionsCompleted = assertionThread match {
      case Some(thread) =>
        try {
          thread.join(AssertionThreadTimeout.toMillis)
          !thread.isAlive // Returns true if thread completed, false if still running
        } catch {
          case _: InterruptedException =>
            println("[WARNING] Assertion thread was interrupted while waiting for completion")
            false
        }
      case None => true // No assertions to run, so consider them "completed"
    }

    // If assertions didn't complete, add a failure result
    if (!assertionsCompleted && config.assertions.nonEmpty) {
      // Add failure results for all assertions that didn't complete
      config.assertions.foreach { assertion =>
        if (!assertionResults.contains(assertion.name)) {
          assertionResults += (assertion.name -> AssertionResult.Failure(
            "Assertion did not complete within timeout or was interrupted",
            None
          ))
        }
      }
    }

    val finalStatus = podFailure match {
      case Some(failureMsg) =>
        println(s"[FAILED] Pod failure detected: $failureMsg")
        JobSetStatus.Failed
      case None =>
        jobStatus
    }

    // Capture debug info if test failed OR if assertions failed
    val hasAssertionFailures =
      assertionResults.values.exists(_.isInstanceOf[AssertionResult.Failure])
    if (finalStatus != JobSetStatus.Success || hasAssertionFailures) {
      println("[DEBUG] Test or assertions failed, capturing debug information...")

      // Print which assertions failed for clarity
      if (hasAssertionFailures) {
        println("[DEBUG] Failed assertions:")
        assertionResults.foreach { case (name, result) =>
          result match {
            case AssertionResult.Failure(msg, _) =>
              println(s"  - $name: $msg")
            case _ =>
          }
        }
      }

      podMonitor.captureDebugInfo()
      podMonitor.printCapturedLogs()
    }

    TestResult(jobSetId, queueName, finalStatus, assertionResults)
  }

  private def buildDockerCommand(
      imageName: String,
      volumeMounts: Seq[String],
      masterUrl: String,
      testName: String,
      queueName: String,
      jobSetId: String,
      sparkConfs: Map[String, String],
      appResource: String,
      lookoutUrl: String,
      pythonScript: Option[String]
  ): Seq[String] = {
    val baseCommand = Seq(
      "docker",
      "run",
      "--rm",
      "--network",
      "host"
    ) ++ volumeMounts ++ Seq(
      imageName,
      "/opt/spark/bin/spark-class",
      "org.apache.spark.deploy.ArmadaSparkSubmit",
      "--master",
      masterUrl,
      "--deploy-mode",
      "cluster",
      "--name",
      s"e2e-$testName"
    )

    val commandWithApp = pythonScript match {
      case Some(_) => baseCommand // Python: no --class needed
      case None    => baseCommand ++ Seq("--class", "org.apache.spark.examples.SparkPi")
    }

    val defaultConfs = Map(
      "spark.armada.internalUrl"             -> "armada-server.armada:50051",
      "spark.armada.queue"                   -> queueName,
      "spark.armada.jobSetId"                -> jobSetId,
      "spark.executor.instances"             -> "2",
      "spark.armada.container.image"         -> imageName,
      "spark.armada.lookouturl"              -> lookoutUrl,
      "spark.armada.driver.limit.cores"      -> "200m",
      "spark.armada.driver.limit.memory"     -> "450Mi",
      "spark.armada.driver.request.cores"    -> "200m",
      "spark.armada.driver.request.memory"   -> "450Mi",
      "spark.armada.executor.limit.cores"    -> "100m",
      "spark.armada.executor.limit.memory"   -> "510Mi",
      "spark.armada.executor.request.cores"  -> "100m",
      "spark.armada.executor.request.memory" -> "510Mi",
      "spark.local.dir"                      -> "/tmp",
      "spark.home"                           -> "/opt/spark"
    )

    val allConfs = defaultConfs ++ sparkConfs
    val confArgs = allConfs.flatMap { case (key, value) =>
      Seq("--conf", s"$key=$value")
    }.toSeq

    commandWithApp ++ confArgs ++ Seq(appResource, "100")
  }

  private def buildVolumeMounts(): Seq[String] = {
    val userDir = System.getProperty("user.dir")
    val e2eDir  = new File(s"$userDir/src/test/resources/e2e")

    if (e2eDir.exists() && e2eDir.isDirectory) {
      Seq("-v", s"${e2eDir.getAbsolutePath}:/opt/spark/work-dir/src/test/resources/e2e:ro")
    } else {
      Seq.empty
    }
  }
}
