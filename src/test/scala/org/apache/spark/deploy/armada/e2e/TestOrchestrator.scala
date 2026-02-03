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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.DeploymentModeHelper

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

  private def waitForPod(
      selector: String,
      namespace: String,
      failureMessage: String,
      jobCompleted: () => Boolean
  ): Boolean = {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    var podRunning      = false
    var attempts        = 0
    val maxWaitAttempts = 60 // 2 minutes max to wait for pod

    while (!podRunning && !jobCompleted() && attempts < maxWaitAttempts) {
      try {
        val pods = Await.result(
          k8sClient.getPodsByLabel(selector, namespace),
          10.seconds
        )
        pods.headOption.foreach { pod =>
          if (pod.getStatus != null && pod.getStatus.getPhase == "Running") {
            podRunning = true
          }
        }
      } catch {
        case _: Exception =>
      }

      if (!podRunning) {
        Thread.sleep(2000)
        attempts += 1
      }
    }

    podRunning
  }

  /** Runs all test assertions while the Spark job is still running (driver/executors present).
    *
    * Parallel assertion flow:
    *   1. Wait for the relevant pod(s) to be Running (driver in cluster mode, executors in client
    *      mode).
    *   2. Once pods are up, run every assertion in parallel via validateAllAssertions.
    *   3. Each assertion runs in its own Future with a 1s sampling loop, so we can observe
    *      transient state (e.g. peak executor count) that might disappear after job completion.
    */
  private def runAssertionsWhileJobRunning(
      assertions: Seq[TestAssertion],
      context: TestContext,
      queueName: String,
      jobCompleted: () => Boolean,
      isClientMode: Boolean = false
  ): Map[String, AssertionResult] = {
    val assertionContext = AssertionContext(
      context.namespace,
      queueName,
      context.testId,
      k8sClient,
      armadaClient
    )

    val (selector, failureMessage) = if (!isClientMode) {
      // Wait for Spark driver pod to be scheduled and running
      (
        s"spark-role=driver,test-id=${context.testId}",
        "Driver pod never started, Armada may not have scheduled it"
      )
    } else {
      // In client mode, wait for at least one executor pod to start
      (
        s"spark-role=executor,test-id=${context.testId}",
        "Executor pods never started, Armada may not have scheduled them"
      )
    }

    if (!waitForPod(selector, context.namespace, failureMessage, jobCompleted)) {
      return assertions.map { assertion =>
        assertion.name -> AssertionResult.Failure(failureMessage)
      }.toMap
    }

    validateAllAssertions(assertions, assertionContext, jobCompleted)
  }

  /** Runs a single assertion in a loop until it succeeds, times out, or the job completes.
    *
    * Exit conditions:
    *   - Assertion returns Success → return that result and exit.
    *   - maxWaitTime elapsed: return last result (success or failure).
    *   - Job completed and gracePeriodAfterCompletion has elapsed: return last result.
    */
  private def runAssertion(
      assertion: TestAssertion,
      context: AssertionContext,
      jobCompleted: () => Boolean,
      maxWaitTime: FiniteDuration,
      gracePeriodAfterCompletion: FiniteDuration
  )(implicit ec: ExecutionContext): Future[(String, AssertionResult)] = Future {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val startTime                                  = System.currentTimeMillis()
    var jobCompletedTime                           = Option.empty[Long]
    var lastResult: AssertionResult                = AssertionResult.Failure("Not yet checked")
    var outcome: Option[(String, AssertionResult)] = None

    while (outcome.isEmpty) {
      val now       = System.currentTimeMillis()
      val completed = jobCompleted()
      if (completed && jobCompletedTime.isEmpty) jobCompletedTime = Some(now)

      // Stop if we've exceeded max wait time, or job finished and grace period after completion has passed
      val graceExpired =
        jobCompletedTime.exists(t => (now - t) > gracePeriodAfterCompletion.toMillis)
      val timeout = (now - startTime) > maxWaitTime.toMillis

      if (timeout || graceExpired) {
        val finalResult = lastResult match {
          case AssertionResult.Failure(msg) if jobCompletedTime.isDefined =>
            AssertionResult.Failure(s"$msg (job completed before assertion could pass)")
          case r => r
        }
        outcome = Some((assertion.name, finalResult))
      } else {
        try {
          val result = Await.result(assertion.assert(context)(ec), 10.seconds)
          lastResult = result
          result match {
            case AssertionResult.Success =>
              outcome = Some((assertion.name, result))
            case _ =>
            // Stay in loop; we'll retry after 1s (state may change, e.g. more executors)
          }
        } catch {
          case ex: Exception =>
            lastResult = AssertionResult.Failure(ex.getMessage)
        }
        if (outcome.isEmpty) Thread.sleep(1000)
      }
    }
    outcome.get
  }

  /** Runs every assertion in parallel: one Future per assertion, each running runAssertion. */
  private def validateAllAssertions(
      assertions: Seq[TestAssertion],
      context: AssertionContext,
      jobCompleted: () => Boolean
  ): Map[String, AssertionResult] = {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val maxWaitTime                = 60.seconds
    val gracePeriodAfterCompletion = 20.seconds
    val overallTimeout             = maxWaitTime + gracePeriodAfterCompletion + 15.seconds

    val futures = assertions.map { assertion =>
      runAssertion(
        assertion,
        context,
        jobCompleted,
        maxWaitTime,
        gracePeriodAfterCompletion
      )
    }
    val results = Await.result(Future.sequence(futures), overallTimeout)
    results.toMap
  }

  def runTest(name: String, config: TestConfig): Future[TestResult] = {
    val context = TestContext(name)
    val testJobSetId =
      s"e2e-${name.toLowerCase.replaceAll("[^a-z0-9]", "-")}-${System.currentTimeMillis()}"
    val queueName = s"${config.baseQueueName}-${context.queueSuffix}"

    // Determine deployment mode type
    val sparkConf = new SparkConf(false)
    config.sparkConfs.foreach { case (key, value) => sparkConf.set(key, value) }
    val modeHelper   = DeploymentModeHelper(sparkConf)
    val modeType     = modeHelper.toString
    val isClientMode = !modeHelper.isDriverInCluster

    println(s"\n========== Starting E2E Test: $name ==========")
    println(s"Test ID: ${context.testId}")
    println(s"Namespace: ${context.namespace}")
    println(s"Queue: $queueName")
    println(s"Deployment Mode: $modeType")

    val resultFuture = for {
      _ <- k8sClient.createNamespace(context.namespace)
      _ <- armadaClient.ensureQueueExists(queueName).recoverWith { case ex =>
        throw new RuntimeException(s"Failed to ensure queue $queueName", ex)
      }
      _      <- submitJob(testJobSetId, queueName, name, config, context, modeHelper)
      result <- watchJob(queueName, testJobSetId, config, context, isClientMode)
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
      context: TestContext,
      modeHelper: DeploymentModeHelper
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

    val deployMode = if (modeHelper.isDriverInCluster) "cluster" else "client"

    val enhancedSparkConfs = config.sparkConfs ++ Map(
      "spark.armada.pod.labels"           -> mergedLabels,
      "spark.armada.scheduling.namespace" -> context.namespace,
      "spark.submit.deployMode"           -> deployMode
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
      config.pythonScript,
      modeHelper
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
      // In client mode, spark-submit runs until application completes, so use longer timeout
      val timeout = if (!modeHelper.isDriverInCluster) jobWatchTimeout else jobSubmitTimeout
      val result  = ProcessExecutor.executeWithResult(dockerCommand, timeout)

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
      context: TestContext,
      isClientMode: Boolean = false
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

    // If we have assertions, run them after driver/executors start but while job is running
    val assertionThread = if (config.assertions.nonEmpty) {
      val thread = new Thread(() => {
        // Wait for driver (cluster mode) or executors (client mode) to start, then run assertions while job is active
        assertionResults = runAssertionsWhileJobRunning(
          config.assertions,
          context,
          queueName,
          jobCompleted = () => jobCompleted,
          isClientMode = isClientMode
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
          assertionResults = assertionResults + (assertion.name -> AssertionResult.Failure(
            "Assertion did not complete within timeout or was interrupted"
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
            case AssertionResult.Failure(msg) =>
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
      pythonScript: Option[String],
      modeHelper: DeploymentModeHelper,
      appArgs: Seq[String] = Seq("100")
  ): Seq[String] = {
    val deployMode   = if (modeHelper.isDriverInCluster) "cluster" else "client"
    val isClientMode = !modeHelper.isDriverInCluster

    val baseCommand = Seq(
      "docker",
      "run",
      "--rm",
      "--network",
      "host"
    ) ++ volumeMounts ++ Seq(
      imageName,
      "/opt/spark/bin/spark-class",
      "org.apache.spark.deploy.SparkSubmit",
      "--master",
      masterUrl,
      "--deploy-mode",
      deployMode,
      "--name",
      s"e2e-$testName"
    )

    val commandWithApp = pythonScript match {
      case Some(_) => baseCommand // Python: no --class needed
      case None    => baseCommand ++ Seq("--class", "org.apache.spark.examples.SparkPi")
    }

    val baseDefaultConfs = Map(
      "spark.armada.queue"                   -> queueName,
      "spark.armada.jobSetId"                -> jobSetId,
      "spark.executor.instances"             -> "2",
      "spark.armada.container.image"         -> imageName,
      "spark.armada.lookouturl"              -> lookoutUrl,
      "spark.armada.driver.limit.cores"      -> "200m",
      "spark.armada.driver.limit.memory"     -> "1Gi",
      "spark.armada.driver.request.cores"    -> "200m",
      "spark.armada.driver.request.memory"   -> "1Gi",
      "spark.armada.executor.limit.cores"    -> "100m",
      "spark.armada.executor.limit.memory"   -> "510Mi",
      "spark.armada.executor.request.cores"  -> "100m",
      "spark.armada.executor.request.memory" -> "510Mi",
      "spark.local.dir"                      -> "/tmp",
      "spark.home"                           -> "/opt/spark",
      "spark.driver.extraJavaOptions"        -> "-XX:-UseContainerSupport",
      "spark.executor.extraJavaOptions"      -> "-XX:-UseContainerSupport"
    )

    // Add deploy-mode specific configs based on modeHelper
    val deployModeConfs = if (isClientMode) {
      // In client mode, driver runs externally, so we need to set spark.driver.host
      // For E2E tests running on kind cluster, the driver host is always 172.18.0.1
      // (the Docker bridge network gateway IP that kind uses)
      Map(
        "spark.driver.host" -> "172.18.0.1",
        "spark.driver.port" -> "7078"
      )
    } else {
      // In cluster mode, driver runs in a pod, so use internal URL
      Map(
        "spark.armada.internalUrl" -> "armada://armada-server.armada:50051"
      )
    }

    val defaultConfs = baseDefaultConfs ++ deployModeConfs

    val allConfs = defaultConfs ++ sparkConfs
    val confArgs = allConfs.flatMap { case (key, value) =>
      Seq("--conf", s"$key=$value")
    }.toSeq

    commandWithApp ++ confArgs ++ (appResource +: appArgs)
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
