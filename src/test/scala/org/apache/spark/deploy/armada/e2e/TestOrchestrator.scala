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
import java.util.concurrent.ScheduledFuture
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

  private def startDiagnosticLogging(namespace: String, jobSetId: String): ScheduledFuture[_] = {
    import java.util.concurrent.{Executors, TimeUnit}

    val scheduler = Executors.newSingleThreadScheduledExecutor()
    var logCount  = 0

    val task = new Runnable {
      def run(): Unit = {
        logCount += 1
        println(s"\n[DIAGNOSTIC] ===== Pod Status Check #$logCount for jobSetId=$jobSetId =====")
        println(s"[DIAGNOSTIC] Timestamp: ${java.time.Instant.now()}")

        try {
          // Get all pods in the namespace
          val podCmd        = Seq("kubectl", "get", "pods", "-n", namespace, "-o", "wide")
          val podListResult = ProcessExecutor.execute(podCmd, 10.seconds)
          println(s"[DIAGNOSTIC] Pod Status:\n${podListResult.stdout}")

          // Try to get logs from any pods that exist
          val lines = podListResult.stdout.split("\n")
          val podNames = if (lines.length > 1) {
            lines
              .drop(1)
              .map(_.trim)
              .filter(_.nonEmpty)
              .map(line => line.split("\\s+").headOption.getOrElse(""))
              .filter(_.nonEmpty)
              .toList
          } else {
            List.empty[String]
          }

          println(s"[DIAGNOSTIC] Found ${podNames.size} pods in namespace $namespace")

          // Get logs for each pod
          podNames.foreach { podName =>
            println(s"\n[DIAGNOSTIC] ----- Pod: $podName -----")

            try {
              // First check pod status
              val statusCmd = Seq("kubectl", "get", "pod", podName, "-n", namespace, "-o", "json")
              val statusResult = ProcessExecutor.execute(statusCmd, 5.seconds)
              if (statusResult.exitCode == 0) {
                // Parse container statuses
                if (statusResult.stdout.contains("\"waiting\"")) {
                  println("[DIAGNOSTIC] Pod is waiting/initializing")
                } else if (
                  statusResult.stdout
                    .contains("\"terminated\"") || statusResult.stdout.contains("Error")
                ) {
                  println("[DIAGNOSTIC] Pod has terminated/errored - attempting to get logs")
                }
              }

              // Get container logs - for failed pods we might need to check previous logs
              val logsCmd    = Seq("kubectl", "logs", podName, "-n", namespace, "--tail=100")
              val logsResult = ProcessExecutor.execute(logsCmd, 5.seconds)
              if (logsResult.exitCode == 0 && logsResult.stdout.trim.nonEmpty) {
                println(s"[DIAGNOSTIC] Logs:\n${logsResult.stdout}")
              } else {
                // Try to get previous logs if container restarted/failed
                val prevLogsCmd =
                  Seq("kubectl", "logs", podName, "-n", namespace, "--previous", "--tail=100")
                val prevLogsResult = ProcessExecutor.execute(prevLogsCmd, 5.seconds)
                if (prevLogsResult.exitCode == 0 && prevLogsResult.stdout.trim.nonEmpty) {
                  println(s"[DIAGNOSTIC] Previous Container Logs:\n${prevLogsResult.stdout}")
                } else {
                  println(s"[DIAGNOSTIC] No logs available. Error: ${logsResult.stderr}")

                  // Get describe output for more info
                  val describeCmd    = Seq("kubectl", "describe", "pod", podName, "-n", namespace)
                  val describeResult = ProcessExecutor.execute(describeCmd, 5.seconds)
                  if (describeResult.exitCode == 0) {
                    val lines       = describeResult.stdout.split("\n")
                    val eventsIndex = lines.indexWhere(_.contains("Events:"))
                    if (eventsIndex >= 0) {
                      println("[DIAGNOSTIC] Pod Events from describe:")
                      println(lines.drop(eventsIndex).take(20).mkString("\n"))
                    }
                  }
                }
              }
            } catch {
              case e: Exception =>
                println(s"[DIAGNOSTIC] Error getting logs for $podName: ${e.getMessage}")
            }
          }

          // Get namespace events
          println(s"\n[DIAGNOSTIC] ----- Recent Events -----")
          try {
            val eventsCmd = Seq(
              "kubectl",
              "get",
              "events",
              "-n",
              namespace,
              "--sort-by=.lastTimestamp",
              "-o",
              "wide"
            )
            val eventsResult = ProcessExecutor.execute(eventsCmd, 5.seconds)
            if (eventsResult.exitCode == 0 && eventsResult.stdout.nonEmpty) {
              val eventLines = eventsResult.stdout.split("\n")
              // Show last 15 events
              println(eventLines.takeRight(15).mkString("\n"))
            }
          } catch {
            case e: Exception =>
              println(s"[DIAGNOSTIC] Error getting events: ${e.getMessage}")
          }

        } catch {
          case e: Exception =>
            println(s"[DIAGNOSTIC] Error during diagnostic logging: ${e.getMessage}")
        }

        println(s"[DIAGNOSTIC] ===== End of Pod Status Check #$logCount =====\n")
      }
    }

    // Run diagnostics every 5 seconds, starting after 5 seconds
    scheduler.scheduleAtFixedRate(task, 5, 5, TimeUnit.SECONDS)
  }

  def runTest(name: String, config: TestConfig): Future[TestResult] = {
    val context = TestContext(name)
    val testJobSetId =
      s"e2e-${name.toLowerCase.replaceAll("[^a-z0-9]", "-")}-${System.currentTimeMillis()}"
    val queueName = s"${config.baseQueueName}-${context.queueSuffix}"

    println(s"\n[TEST] ========== Starting E2E Test: $name ==========")
    println(s"[TEST] Test ID: ${context.testId}")
    println(s"[TEST] Namespace: ${context.namespace}")
    println(s"[TEST] Queue: $queueName")
    println(s"[TEST] Timestamp: ${java.time.Instant.now()}")

    val resultFuture = for {
      _ <- {
        println(s"[TEST] Creating namespace: ${context.namespace}")
        k8sClient.createNamespace(context.namespace)
      }
      _ <- {
        println(s"[TEST] Ensuring queue exists: $queueName")
        armadaClient.ensureQueue(queueName).recoverWith { case ex =>
          println(s"[TEST] ERROR: Failed to ensure queue $queueName: ${ex.getMessage}")
          ex.printStackTrace()

          // Try to create it directly as a fallback
          println(s"[TEST] Attempting direct queue creation as fallback...")
          val armadactlPath = sys.props.get("armadactl.path").getOrElse("armadactl")
          val cmd           = s"$armadactlPath create queue $queueName --armadaUrl localhost:30002"
          println(s"[TEST] Executing fallback command: $cmd")

          Future {
            try {
              val result = scala.sys.process.Process(cmd).!!
              println(s"[TEST] Direct queue creation result: $result")
              Thread.sleep(5000) // Give it time to propagate
            } catch {
              case e: Exception =>
                println(s"[TEST] Direct queue creation also failed: ${e.getMessage}")
                throw new RuntimeException(
                  s"Failed to create queue $queueName via both methods",
                  ex
                )
            }
          }
        }
      }
      _ <- {
        // Add a longer delay and verify queue from container perspective
        println(s"[TEST] Waiting for queue to propagate...")
        Thread.sleep(20000) // 20 seconds - give plenty of time for propagation

        // Double-check queue exists by trying to get it again
        println(s"[TEST] Verifying queue $queueName is accessible...")
        armadaClient.getQueue(queueName).flatMap { queueOpt =>
          if (queueOpt.isEmpty) {
            println(s"[TEST] WARNING: Queue $queueName still not visible after creation!")
            println(s"[TEST] Waiting additional 15 seconds...")
            Thread.sleep(15000)
            armadaClient.getQueue(queueName).map { retryOpt =>
              if (retryOpt.isEmpty) {
                println(s"[TEST] ERROR: Queue $queueName still not visible after 35 seconds!")
                // Try one more time to list all queues to see what's available
                println(s"[TEST] Listing all available queues...")
                val listCmd =
                  s"${sys.props.get("armadactl.path").getOrElse("armadactl")} get queues --armadaUrl localhost:30002"
                try {
                  val result = scala.sys.process.Process(listCmd).!!
                  println(s"[TEST] Available queues:\n$result")
                } catch {
                  case e: Exception =>
                    println(s"[TEST] Failed to list queues: ${e.getMessage}")
                }
              } else {
                println(s"[TEST] Queue $queueName finally became visible after retry")
              }
            }
          } else {
            println(s"[TEST] Queue $queueName verified as accessible")
            Future.successful(())
          }
        }
      }
      _ <- {
        println(s"[TEST] Submitting job to queue: $queueName")
        submitJob(testJobSetId, queueName, name, config, context)
      }
      result <-
        if (config.assertions.nonEmpty) {
          watchJobWithAssertions(queueName, testJobSetId, config, context)
        } else {
          // Start diagnostic logging in background
          val diagnosticLogger = startDiagnosticLogging(context.namespace, testJobSetId)

          armadaClient
            .watchJobSet(queueName, testJobSetId, jobWatchTimeout)
            .map { status =>
              // Stop diagnostic logging
              diagnosticLogger.cancel(false)
              println(s"* Job completed with status: $status")
              TestResult(testJobSetId, queueName, status)
            }
            .recover { case ex =>
              // Stop diagnostic logging on error
              diagnosticLogger.cancel(false)
              throw ex
            }
        }
    } yield result

    // Ensure cleanup happens regardless of test outcome
    resultFuture
      .andThen {
        case scala.util.Failure(ex) =>
          println(
            s"[TEST] ERROR: Test failed with exception: ${ex.getClass.getSimpleName}: ${ex.getMessage}"
          )
          ex.printStackTrace()
        case _ =>
      }
      .andThen { case _ =>
        cleanupTest(context, queueName)
      }
      .map { result =>
        println(s"\n[TEST] ========== Test Completed: $name ==========")
        println(s"[TEST] Final Status: ${result.status}")
        println(s"[TEST] JobSetId: ${result.jobSetId}")
        println(s"[TEST] Queue: ${result.queueName}")
        println(s"[TEST] Duration: ${(System.currentTimeMillis() - context.startTime) / 1000}s")
        if (result.assertionResults.nonEmpty) {
          println(s"** Assertions: ${result.assertionResults.size} total")
        }
        result
      }
  }

  private def cleanupTest(context: TestContext, queueName: String): Future[Unit] = {
    println(s"[CLEANUP] Starting cleanup for test: ${context.testName}")
    for {
      _ <- k8sClient.deleteNamespace(context.namespace).recover { case ex =>
        println(s"[CLEANUP] Failed to delete namespace ${context.namespace}: ${ex.getMessage}")
        ()
      }
      _ <- armadaClient.deleteQueue(queueName).recover { case ex =>
        println(s"[CLEANUP] Failed to delete queue $queueName: ${ex.getMessage}")
        ()
      }
    } yield {
      println(s"[CLEANUP] Cleanup completed for ${context.testName}")
    }
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
    println(s"[SUBMIT] Using spark-examples JAR: $sparkExamplesJar")
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

    println(s"[SUBMIT] Starting submission for test: $testName")
    println(s"[SUBMIT] Queue: $queueName")
    println(s"[SUBMIT] JobSetId: $jobSetId")
    println(s"[SUBMIT] Container image: ${config.imageName}")
    println(s"[SUBMIT] Armada master: ${config.masterUrl}")
    println(s"[SUBMIT] Command length: ${dockerCommand.length} args")
    println(s"[SUBMIT] Full command: ${dockerCommand.mkString(" ")}")

    val submitStart    = System.currentTimeMillis()
    val result         = ProcessExecutor.execute(dockerCommand, jobSubmitTimeout)
    val submitDuration = (System.currentTimeMillis() - submitStart) / 1000

    println(
      s"[SUBMIT] Submission completed in ${submitDuration}s with exit code: ${result.exitCode}"
    )

    // Log stdout if present
    if (result.stdout.nonEmpty) {
      println("[SUBMIT] === STDOUT START ===")
      println(result.stdout)
      println("[SUBMIT] === STDOUT END ===")

      // Try to extract job IDs from output
      val driverJobIdPattern   = """Submitted driver job with ID: ([a-z0-9]+)""".r
      val executorJobIdPattern = """Submitted executor job with ID: ([a-z0-9]+)""".r

      driverJobIdPattern.findFirstMatchIn(result.stdout).foreach { m =>
        println(s"[SUBMIT] Driver job ID: ${m.group(1)}")
      }
      executorJobIdPattern.findAllMatchIn(result.stdout).foreach { m =>
        println(s"[SUBMIT] Executor job ID: ${m.group(1)}")
      }
    }

    // Log stderr if present (even on success, as it might contain useful info)
    if (result.stderr.nonEmpty) {
      println("[SUBMIT] === STDERR START ===")
      println(result.stderr)
      println("[SUBMIT] === STDERR END ===")
    }

    if (result.exitCode != 0) {
      println(s"[SUBMIT] ERROR: Submit failed with exit code: ${result.exitCode}")
      // Try to find the actual error in stderr or stdout
      val errorPattern = """Exception|Error|Failed|FAILED""".r
      val relevantLines = (result.stdout + "\n" + result.stderr)
        .split("\n")
        .filter(line => errorPattern.findFirstIn(line).isDefined)
        .take(10)

      if (relevantLines.nonEmpty) {
        println("[SUBMIT] Relevant error lines:")
        relevantLines.foreach(line => println(s"  > $line"))
      }

      throw new RuntimeException(s"Spark submit failed with exit code ${result.exitCode}")
    } else {
      println(s"[SUBMIT] Job submission successful")
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

    // Start diagnostic logging in background
    val diagnosticLogger = startDiagnosticLogging(context.namespace, jobSetId)

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

    val result = for {
      jobStatus        <- jobPromise.future
      assertionResults <- assertionPromise.future
    } yield TestResult(jobSetId, queueName, jobStatus, assertionResults)

    // Stop diagnostic logging when test completes
    result.onComplete { _ =>
      diagnosticLogger.cancel(false)
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

    // Log the exact queue name being used
    println(s"[SUBMIT] Using queue name: '$queueName' (length: ${queueName.length})")

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
        println("[SUBMIT] Detected CI environment - adjusting Armada URLs")
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
