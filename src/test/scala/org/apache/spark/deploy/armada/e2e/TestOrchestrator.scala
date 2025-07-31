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
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class TestConfig(
    baseQueueName: String,
    imageName: String,
    masterUrl: String,
    lookoutUrl: String,
    scalaVersion: String,
    sparkVersion: String,
    sparkConfs: Map[String, String] = Map.empty,
    assertions: Seq[TestAssertion] = Seq.empty
)

/** Manages test isolation with unique namespace and queue per test. Ensures cleanup of resources
  * after test completion.
  */
case class TestContext(
    testName: String,
    testId: String = UUID.randomUUID().toString.take(8)
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

  private val jobSubmitTimeout = 30.seconds
  private val jobWatchTimeout  = 240.seconds

  private def delay(duration: Duration): Future[Unit] = {
    val promise = Promise[Unit]()
    val timer   = new java.util.Timer()
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

  def runTest(name: String, config: TestConfig): Future[TestResult] = {
    val context = TestContext(name)
    val testJobSetId =
      s"e2e-${name.toLowerCase.replaceAll("[^a-z0-9]", "-")}-${System.currentTimeMillis()}"
    val queueName = s"${config.baseQueueName}-${context.queueSuffix}"

    println(s"* Starting E2E Test: $name")
    println(s"** Test ID: ${context.testId}")
    println(s"** Namespace: ${context.namespace}")
    println(s"** Queue: $queueName")

    val resultFuture = for {
      _ <- k8sClient.createNamespace(context.namespace)
      _ <- armadaClient.ensureQueue(queueName)
      _ <- submitJob(testJobSetId, queueName, name, config, context)
      result <-
        if (config.assertions.nonEmpty) {
          watchJobWithAssertions(queueName, testJobSetId, config, context)
        } else {
          armadaClient.watchJobSet(queueName, testJobSetId, jobWatchTimeout).map { status =>
            println(s"* Job completed with status: $status")
            TestResult(testJobSetId, queueName, status)
          }
        }
    } yield result

    // Ensure cleanup happens regardless of test outcome
    resultFuture
      .andThen { case _ =>
        cleanupTest(context, queueName)
      }
      .map { result =>
        println(s"* Test Completed: $name")
        println(s"** Status: ${result.status}")
        println(s"** JobSetId: ${result.jobSetId}")
        println(s"** Queue: ${result.queueName}")
        if (result.assertionResults.nonEmpty) {
          println(s"** Assertions: ${result.assertionResults.size} total")
        }
        result
      }
  }

  private def cleanupTest(context: TestContext, queueName: String): Future[Unit] = {
    println(s"* Cleaning up test resources for ${context.testName}")
    for {
      _ <- k8sClient.deleteNamespace(context.namespace).recover { case _ => () }
      _ <- armadaClient.deleteQueue(queueName).recover { case _ => () }
    } yield {
      println(s"** Cleanup completed for ${context.testName}")
    }
  }

  private def submitJob(
      jobSetId: String,
      queueName: String,
      testName: String,
      config: TestConfig,
      context: TestContext
  ): Future[Unit] = Future {
    val sparkExamplesJar =
      s"local:///opt/spark/examples/jars/spark-examples_${config.scalaVersion}-${config.sparkVersion}.jar"
    val volumeMounts = buildVolumeMounts()

    // Merge test context labels with config labels
    val mergedLabels = config.sparkConfs
      .get("spark.armada.pod.labels")
      .map(existing => s"$existing,${context.labels.map { case (k, v) => s"$k=$v" }.mkString(",")}")
      .getOrElse(context.labels.map { case (k, v) => s"$k=$v" }.mkString(","))

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

    println(s"* Submitting $testName job")
    println(s"** Queue: $queueName")
    println(s"** JobSetId: $jobSetId")
    println(s"** Command: ${dockerCommand.mkString(" ")}")

    val result = ProcessExecutor.execute(dockerCommand, jobSubmitTimeout)

    // Log stdout if present
    if (result.stdout.nonEmpty) {
      println("* Submit Output")
      println(result.stdout)
    }

    // Log stderr if present (even on success, as it might contain useful info)
    if (result.stderr.nonEmpty) {
      println("* Submit Stderr")
      println(result.stderr)
    }

    if (result.exitCode != 0) {
      println(s"* Submit Failed with exit code: ${result.exitCode}")
      throw new RuntimeException(s"Spark submit failed with exit code ${result.exitCode}")
    } else {
      println(s"* Submit Succeeded")
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
      _ <- k8sClient.waitForPodRunning(labelSelector, context.namespace, 150.seconds)
      _ = println("* Driver pod is running, waiting for resources to be created...")
      _ <- delay(3.seconds) // Brief delay for resource creation
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

    for {
      jobStatus        <- jobPromise.future
      assertionResults <- assertionPromise.future
    } yield TestResult(jobSetId, queueName, jobStatus, assertionResults)
  }

  private def buildDockerCommand(
      imageName: String,
      volumeMounts: Seq[String],
      masterUrl: String,
      testName: String,
      queueName: String,
      jobSetId: String,
      sparkConfs: Map[String, String],
      sparkExamplesJar: String,
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

    val allConfs = defaultConfs ++ sparkConfs
    val confArgs = allConfs.flatMap { case (key, value) =>
      Seq("--conf", s"$key=$value")
    }.toSeq

    baseCommand ++ confArgs ++ Seq(sparkExamplesJar, "100")
  }

  private def buildVolumeMounts(): Seq[String] = {
    val userDir          = System.getProperty("user.dir")
    val testResourcesDir = new File(s"$userDir/src/test/resources")

    if (!testResourcesDir.exists() || !testResourcesDir.isDirectory) {
      return Seq.empty
    }

    val mountPatterns = Seq(
      "e2e" -> "/opt/spark/work-dir/src/test/resources/e2e"
    )

    mountPatterns.flatMap { case (subDir, containerPath) =>
      val hostDir = new File(testResourcesDir, subDir)
      if (hostDir.exists() && hostDir.isDirectory) {
        Seq("-v", s"${hostDir.getAbsolutePath}:$containerPath:ro")
      } else {
        Seq.empty
      }
    }
  }
}
