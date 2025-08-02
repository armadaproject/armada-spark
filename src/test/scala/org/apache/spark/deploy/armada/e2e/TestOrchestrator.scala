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
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import TestConstants._
import scala.util.{Failure, Success, Try}

// Extension for Future.never
object FutureExtensions {
  def never[T]: Future[T] = Promise[T]().future
}

case class TestConfig(
    baseQueueName: String,
    imageName: String,
    masterUrl: String,
    lookoutUrl: String,
    scalaVersion: String,
    sparkVersion: String,
    sparkConfs: Map[String, String] = Map.empty,
    assertions: Seq[TestAssertion] = Seq.empty,
    failFastOnPodFailure: Boolean = true
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

class AssertionFailedException(message: String) extends Exception(message)

/** Orchestrates end-to-end testing of Spark jobs on Armada.
  *
  * This class manages the complete lifecycle of E2E tests including:
  *   - Queue creation and management
  *   - Docker-based Spark job submission
  *   - Job monitoring and status tracking
  *   - Runtime assertion execution (e.g., verifying ingress creation)
  *   - Comprehensive logging of all test stages
  *
  * Tests can include assertions that run while the job is executing, allowing verification of
  * runtime resources like Kubernetes ingresses. Failed assertions will cause the test to fail.
  *
  * @param armadaClient
  *   Client for Armada operations (queue management, job watching)
  * @param k8sClient
  *   Client for Kubernetes operations (pod/ingress verification)
  */
class TestOrchestrator(
    armadaClient: ArmadaOperations,
    k8sClient: K8sOperations
)(implicit ec: ExecutionContext) {

  private val jobSubmitTimeout = JobSubmitTimeout
  private val jobWatchTimeout  = JobWatchTimeout

  private def delay(duration: Duration): Future[Unit] = {
    val promise = Promise[Unit]()
    val timer   = new java.util.Timer(true) // daemon timer to prevent resource leak
    timer.schedule(
      new java.util.TimerTask {
        def run(): Unit = {
          promise.success(())
          timer.cancel()
        }
      },
      duration.toMillis
    )
    promise.future
  }

  private def waitForIngressCreation(
      context: TestContext,
      k8sClient: K8sOperations,
      maxWait: Duration
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val startTime     = System.currentTimeMillis()
    val maxWaitMillis = maxWait.toMillis

    def checkIngress(): Future[Unit] = {
      k8sClient.getPodByLabel(s"test-id=${context.testId}", context.namespace).flatMap {
        case Some(pod) =>
          k8sClient.getIngressForPod(pod.name, context.namespace).flatMap {
            case Some(_) =>
              println(s"* Ingress created for pod ${pod.name}")
              Future.successful(())
            case None if (System.currentTimeMillis() - startTime) > maxWaitMillis =>
              Future.failed(new TimeoutException(s"Ingress not created within $maxWait"))
            case None =>
              // Wait 2 seconds and retry
              delay(IngressCreationCheckInterval).flatMap(_ => checkIngress())
          }
        case None =>
          Future.failed(new RuntimeException("Driver pod disappeared while waiting for ingress"))
      }
    }

    checkIngress()
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
    println(s"Time: ${java.time.LocalTime.now()}")

    val resultFuture = for {
      _ <- {
        k8sClient.createNamespace(context.namespace)
      }
      _ <- {
        // ensureQueueExists creates queue and polls until visible
        armadaClient.ensureQueueExists(queueName).recoverWith { case ex =>
          throw new RuntimeException(s"Failed to ensure queue $queueName", ex)
        }
      }
      _ <- submitJob(testJobSetId, queueName, name, config, context)
      result <-
        if (config.assertions.nonEmpty) {
          watchJobWithAssertions(queueName, testJobSetId, config, context)
        } else {
          watchJobWithPodMonitoring(queueName, testJobSetId, config, context)
        }
    } yield result

    // Ensure cleanup happens regardless of test outcome
    resultFuture
      .andThen {
        case scala.util.Failure(ex) =>
          println(s"Test failed: ${ex.getMessage}")
        case _ =>
      }
      .andThen { case _ =>
        cleanupTest(context, queueName)
      }
      .map { result =>
        println(s"\n========== Test Completed: $name ==========")
        println(s"Status: ${result.status}")
        println(s"Duration: ${(System.currentTimeMillis() - context.startTime) / 1000}s")
        println(s"End Time: ${java.time.LocalTime.now()}")
        if (result.assertionResults.nonEmpty) {
          println(s"** Assertions: ${result.assertionResults.size} total")
        }
        result
      }
  }

  private def cleanupTest(context: TestContext, queueName: String): Future[Unit] = {
    for {
      _ <- k8sClient.deleteNamespace(context.namespace).recover { case ex =>
        ()
      }
      _ <- armadaClient.deleteQueue(queueName).recover { case ex =>
        ()
      }
    } yield {}
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
    val sparkExamplesJar =
      s"local:///opt/spark/examples/jars/spark-examples_${config.scalaVersion}-${config.sparkVersion}.jar"
    val volumeMounts = buildVolumeMounts()

    // Merge test context labels with config labels
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
      sparkExamplesJar,
      config.lookoutUrl
    )

    // Log the submission details
    println(s"\n[SUBMIT] Submitting Spark job via Docker:")
    println(s"[SUBMIT]   Queue: $queueName")
    println(s"[SUBMIT]   JobSetId: $jobSetId")
    println(s"[SUBMIT]   Namespace: ${context.namespace}")
    println(s"[SUBMIT]   Image: ${config.imageName}")
    println(s"[SUBMIT]   Master URL: ${config.masterUrl}")
    println(s"[SUBMIT]   Application JAR: $sparkExamplesJar")
    println(s"[SUBMIT]   Spark config:")
    enhancedSparkConfs.toSeq.sortBy(_._1).foreach { case (key, value) =>
      // Truncate long values for readability
      val displayValue = if (value.length > 100) value.take(100) + "..." else value
      println(s"[SUBMIT]     $key = $displayValue")
    }
    println(s"[SUBMIT] Executing Docker command (${dockerCommand.size} args total)")
    // Properly escape command for shell reproduction
    val escapedCommand = dockerCommand.map { arg =>
      if (arg.contains(" ") || arg.contains("'") || arg.contains("\"")) {
        "'" + arg.replace("'", "'\\''") + "'"
      } else arg
    }
    println(s"[SUBMIT] Full command: ${escapedCommand.mkString(" ")}\n")

    val result = ProcessExecutor.execute(dockerCommand, jobSubmitTimeout)

    if (result.exitCode != 0) {
      // Try to find the actual error in stderr or stdout
      val errorPattern = """Exception|Error|Failed|FAILED""".r
      val relevantLines = (result.stdout + "\n" + result.stderr)
        .split("\n")
        .filter(line => errorPattern.findFirstIn(line).isDefined)
        .take(10)

      println(s"[SUBMIT] ❌ Submit failed with exit code ${result.exitCode}")
      if (relevantLines.nonEmpty) {
        println("[SUBMIT] Relevant error lines:")
        relevantLines.foreach(line => println(s"[SUBMIT]   $line"))
      }

      throw new RuntimeException(s"Spark submit failed with exit code ${result.exitCode}")
    } else {
      println(s"[SUBMIT] Job submitted successfully")
      println(s"[SUBMIT] Now monitoring job in queue '$queueName' with jobSetId '$jobSetId'")
    }
  }

  private def watchJobWithPodMonitoring(
      queueName: String,
      jobSetId: String,
      config: TestConfig,
      context: TestContext
  ): Future[TestResult] = {
    println(s"[MONITOR] Starting job monitoring for jobSetId: $jobSetId")
    println(s"[MONITOR] Pod failure detection: ${if (config.failFastOnPodFailure) "enabled"
      else "disabled"}")

    val podWatcher = new PodWatcher(context.namespace, jobSetId, config.failFastOnPodFailure)

    // Start monitoring pods
    val podFailureFuture = if (config.failFastOnPodFailure) {
      println(s"[MONITOR] Starting pod watcher in namespace: ${context.namespace}")
      podWatcher.start()
    } else {
      FutureExtensions.never
    }

    // Watch the job
    println(s"[MONITOR] Starting Armada job watch with timeout: ${jobWatchTimeout.toSeconds}s")
    val jobFuture = armadaClient.watchJobSet(queueName, jobSetId, jobWatchTimeout)

    // Race between job completion and pod failure
    val resultFuture = Future.firstCompletedOf(
      Seq(
        jobFuture.map { status =>
          println(s"[COMPLETE] Job finished with status=$status at ${java.time.LocalTime.now()}")
          TestResult(jobSetId, queueName, status)
        },
        podFailureFuture.map { failureMsg =>
          println(s"[FAILED] Job failed: $failureMsg at ${java.time.LocalTime.now()}")
          TestResult(jobSetId, queueName, JobSetStatus.Failed)
        }
      )
    )

    // Handle completion
    resultFuture.andThen {
      case Success(result) =>
        podWatcher.stop()
        // Print captured logs only if the test failed
        if (result.status != JobSetStatus.Success) {
          podWatcher.getLogCapture().printCapturedLogs()
        }
      case Failure(ex) =>
        podWatcher.stop()
        podWatcher.getLogCapture().printCapturedLogs()
        println(s"* Job watch failed: ${ex.getMessage}")
    }
  }

  private def watchJobWithAssertions(
      queueName: String,
      jobSetId: String,
      config: TestConfig,
      context: TestContext
  ): Future[TestResult] = {
    val assertionPromise = Promise[Map[String, AssertionResult]]()
    val jobPromise       = Promise[JobSetStatus]()
    val podWatcher       = new PodWatcher(context.namespace, jobSetId, config.failFastOnPodFailure)

    // Start pod monitoring
    val podFailureFuture = if (config.failFastOnPodFailure) {
      podWatcher.start()
    } else {
      FutureExtensions.never
    }

    val assertionContext =
      AssertionContext(jobSetId, context.namespace, context.testId, k8sClient, armadaClient)

    println(s"* Watching job $jobSetId in queue $queueName")
    val jobFuture = armadaClient.watchJobSet(queueName, jobSetId, jobWatchTimeout)
    jobFuture.onComplete { result =>
      result match {
        case Success(status) => println(s"** Job completed with status: $status")
        case Failure(ex)     => println(s"** Job watch failed: ${ex.getMessage}")
      }
      jobPromise.tryComplete(result)
    }

    // Use test-id label to identify pods belonging to this specific test
    val labelSelector = s"test-id=${context.testId}"

    val assertionFuture = for {
      // Look for driver pod with test-id label in the test-specific namespace
      _ <- k8sClient.waitForPodRunning(labelSelector, context.namespace, PodRunningTimeout)
      _ = println("* Driver pod is running, waiting for resources to be created...")
      // Poll for ingress to be created (if ingress assertions are present)
      _ <-
        if (config.assertions.exists(_.isInstanceOf[IngressAssertion])) {
          waitForIngressCreation(context, k8sClient, maxWait = IngressCreationTimeout)
        } else {
          Future.successful(())
        }
      _ = println("* Running assertions...")
      results <- AssertionRunner.runAssertions(config.assertions, assertionContext)
    } yield results

    assertionFuture.onComplete {
      case Success(results) =>
        println(s"** Assertions completed: ${results.size} assertions run")
        val failedAssertions = results.collect { case (name, AssertionResult.Failure(reason, _)) =>
          (name, reason)
        }
        results.foreach { case (name, status) =>
          println(s"  - $name: $status")
        }
        if (failedAssertions.nonEmpty) {
          val failureMessage = failedAssertions
            .map { case (name, reason) => s"$name: $reason" }
            .mkString("Failed assertions:\n", "\n", "")
          assertionPromise.failure(new AssertionFailedException(failureMessage))
        } else {
          assertionPromise.success(results)
        }
      case Failure(ex) =>
        println(s"** Assertion execution failed: ${ex.getMessage}")
        assertionPromise.failure(ex)
    }

    // Handle pod failures
    podFailureFuture.onComplete {
      case Success(failureMsg) =>
        jobPromise.tryFailure(new RuntimeException(s"Pod failure: $failureMsg"))
        assertionPromise.tryFailure(new RuntimeException(s"Pod failure: $failureMsg"))
      case _ => // Ignore
    }

    val result = for {
      jobStatus        <- jobPromise.future
      assertionResults <- assertionPromise.future
    } yield TestResult(jobSetId, queueName, jobStatus, assertionResults)

    // Stop pod watcher and print logs if needed when test completes
    result.onComplete {
      case Success(testResult) =>
        podWatcher.stop()
        if (testResult.status != JobSetStatus.Success) {
          podWatcher.getLogCapture().printCapturedLogs()
        }
      case Failure(_) =>
        podWatcher.stop()
        podWatcher.getLogCapture().printCapturedLogs()
    }

    result
  }

  private def buildDockerCommand(
      imageName: String,
      volumeMounts: Seq[String],
      masterUrl: String,
      testName: String,
      queueName: String,
      jobSetId: String,
      sparkConfs: Map[String, String],
      appJar: String,
      lookoutUrl: String
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
      s"e2e-$testName",
      "--class",
      "org.apache.spark.examples.SparkPi"
    )

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

    // Override internalUrl to use the Kubernetes service name instead of localhost
    val ciAdjustedConfs =
      if (sys.env.get("CI").contains("true") || sys.env.get("GITHUB_ACTIONS").contains("true")) {
        defaultConfs ++ Map(
          "spark.armada.internalUrl" -> "armada-server.armada:50051"
        )
      } else {
        defaultConfs
      }

    val allConfs = ciAdjustedConfs ++ sparkConfs
    val confArgs = allConfs.flatMap { case (key, value) =>
      Seq("--conf", s"$key=$value")
    }.toSeq

    baseCommand ++ confArgs ++ Seq(appJar, "100")
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
