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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.{Seconds, Span}

import java.io.{File, FileInputStream}
import java.util.{Properties, UUID}
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.{Failure, Success, Using}

/** Comprehensive E2E test framework for Armada Spark jobs with different configurations.
  *
  * Each test creates its own queue with a random suffix for isolation.
  *
  * ## Adding New Tests with Templates
  *
  * To add a new test that uses templates or other test resources:
  *
  *   1. **Add your templates/resources**: Place them in `src/test/resources/e2e/` or a new
  *      subdirectory
  *   2. **Update mount patterns**: If using a new subdirectory, add it to
  *      `buildTestResourceVolumeMounts()`
  *   3. **Create your test**: Use relative paths like `src/test/resources/e2e/your-template.yaml`
  *   4. **No other changes needed**: The framework automatically mounts test resources
  */
class ArmadaSparkE2E extends AnyFunSuite with BeforeAndAfterAll with Matchers with TimeLimits {
  private val templateQueueBase = "e2e-template"
  private val jobWatchTimeout   = 240.seconds
  private val testTimeoutSpan   = Span(300, Seconds)
  private val configFile        = new File("src/test/resources/e2e/spark-pi-e2e.conf")
  private val armadaCtl         = new ArmadaCtlWrapper()

  /** Get the path to a template file relative to the e2e test resources directory.
    *
    * @param name
    *   The name of the template file (e.g., "spark-pi-job-template.yaml")
    * @return
    *   The full path to the template file
    */
  private def templatePath(name: String): String =
    s"src/test/resources/e2e/templates/$name"

  private var imageName: String    = _
  private var masterUrl: String    = _
  private var lookoutUrl: String   = _
  private var scalaVersion: String = _
  private var sparkVersion: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val props = new Properties()
    if (configFile.exists()) {
      Using(new FileInputStream(configFile)) { fis =>
        props.load(fis)
      }
    }

    imageName = props.getProperty("container.image", "spark:armada")
    masterUrl = props.getProperty("armada.master", "armada://localhost:30002")
    lookoutUrl = props.getProperty("armada.lookout.url", "http://localhost:30000")
    scalaVersion = props.getProperty("scala.version", "2.13")
    sparkVersion = props.getProperty("spark.version", "3.5.3")

    info(s"Using image: $imageName")
    info(s"Using master: $masterUrl")
    info(s"Using lookout: $lookoutUrl")
    info(s"Using Scala version: $scalaVersion")
    info(s"Using Spark version: $sparkVersion")
  }

  test("Basic SparkPi job", E2ETest) {
    failAfter(testTimeoutSpan) {
      val (testJobSetId, queueName) = prepareTest()

      val result = submitAndWaitForSparkPiJob(
        queueName = queueName,
        jobSetId = testJobSetId,
        testName = "Basic SparkPi",
        imageName = imageName,
        masterUrl = masterUrl,
        sparkConfs = Map(
          "spark.armada.pod.labels" -> "test-type=basic"
        )
      )
      result shouldBe JobSetResult.Success
    }
  }

  test("SparkPi job with node selectors", E2ETest) {
    failAfter(testTimeoutSpan) {
      val (testJobSetId, queueName) = prepareTest()

      val result = submitAndWaitForSparkPiJob(
        queueName = queueName,
        jobSetId = testJobSetId,
        testName = "SparkPi with Node Selectors",
        imageName = imageName,
        masterUrl = masterUrl,
        sparkConfs = Map(
          "spark.armada.scheduling.nodeSelectors" -> "kubernetes.io/hostname=armada-worker",
          "spark.armada.pod.labels"               -> "test-type=node-selector"
        )
      )
      result shouldBe JobSetResult.Success
    }
  }

  test("SparkPi job using job templates", E2ETest) {
    failAfter(testTimeoutSpan) {
      val (testJobSetId, queueName) = prepareTest()

      val result = submitAndWaitForSparkPiJob(
        queueName = queueName,
        jobSetId = testJobSetId,
        testName = "SparkPi with Templates",
        imageName = imageName,
        masterUrl = masterUrl,
        sparkConfs = Map(
          "spark.armada.jobTemplate"            -> templatePath("spark-pi-job-template.yaml"),
          "spark.armada.driver.jobItemTemplate" -> templatePath("spark-pi-driver-template.yaml"),
          "spark.armada.executor.jobItemTemplate" -> templatePath(
            "spark-pi-executor-template.yaml"
          ),
          "spark.armada.pod.labels" -> "test-type=template"
        )
      )
      result shouldBe JobSetResult.Success
    }
  }

  /** Prepare the test environment by creating a unique queue and returning the test job set ID.
    *
    * @return
    *   Tuple containing the test job set ID and the created queue name
    */
  private def prepareTest(): (String, String) = {
    val queueSuffix  = UUID.randomUUID().toString.take(8)
    val queueName    = s"$templateQueueBase-$queueSuffix"
    val testJobSetId = s"e2e-template-${System.currentTimeMillis()}"

    armadaCtl.ensureQueueExists(queueName) match {
      case Success(_)  => info(s"Created queue: $queueName")
      case Failure(ex) => fail(s"Failed to create queue $queueName: ${ex.getMessage}")
    }

    // Due to Armada's eventual consistency, even though we verify the queue exists by fetching it from Armada API,
    // job submission occassionally fails with a "queue not found" error.
    // This might even be a bug in Armada, but for now we add a delay to ensure the queue is ready.
    info(s"Waiting for queue $queueName to be ready...")
    Thread.sleep(10000)

    if (!armadaCtl.verifyQueueExists(queueName)) {
      fail(
        s"Failed to fetch the newly created queue $queueName"
      )
    }

    (testJobSetId, queueName)
  }

  /** Submit a Spark job and wait for completion.
    *
    * This method automatically handles volume mounting for any test resources, making it easy to
    * add new tests with templates or other resources.
    */
  private def submitAndWaitForSparkPiJob(
      queueName: String,
      jobSetId: String,
      testName: String,
      imageName: String,
      masterUrl: String,
      sparkConfs: Map[String, String] = Map.empty
  ): JobSetResult.Value = {

    info(s"Submitting $testName job...")
    info(s"Configuration summary:")
    info(s"  Queue: $queueName")
    info(s"  JobSetId: $jobSetId")
    info(s"  Container Image: $imageName")
    info(s"  Master URL: $masterUrl")

    // Log additional configurations
    sparkConfs.foreach { case (key, value) =>
      info(s"  $key: $value")
    }

    try {
      val sparkExamplesJar =
        s"local:///opt/spark/examples/jars/spark-examples_$scalaVersion-$sparkVersion.jar"

      val volumeMounts = buildTestResourceVolumeMounts()

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
        "spark.armada.executor.request.memory" -> "510Mi"
      )

      // Merge with custom configurations (custom configs override defaults)
      val allConfs = defaultConfs ++ sparkConfs

      // Convert configurations to --conf arguments
      val confArgs = allConfs.flatMap { case (key, value) =>
        Seq("--conf", s"$key=$value")
      }.toSeq

      val finalCommand = baseCommand ++ confArgs ++ Seq(
        sparkExamplesJar,
        "100"
      )

      info(s"Executing docker command for $testName job...")
      val process  = Process(finalCommand)
      val exitCode = process.!

      if (exitCode == 0) {
        info(s"$testName job submitted successfully")
      } else {
        throw new RuntimeException(s"Spark submit failed with exit code $exitCode")
      }

      // Wait for completion using armadactl watch
      info(s"Waiting for jobset $jobSetId to complete...")
      val result = armadaCtl.watchJobSet(
        queueName,
        jobSetId,
        jobWatchTimeout
      )

      result match {
        case JobSetResult.Success =>
          info(s"$testName job completed successfully!")
        case JobSetResult.Failed =>
          info(s"$testName job failed")
        case JobSetResult.Timeout =>
          info(s"watching Armada job until completion timed out after $jobWatchTimeout")
      }

      result

    } catch {
      case ex: Exception =>
        info(s"$testName job submission failed: ${ex.getMessage}")
        JobSetResult.Failed
    }
  }

  /** Build volume mount arguments for Docker to make test resources accessible inside containers.
    *
    * This method automatically mounts directories that contain test resources (templates, configs,
    * etc.) so they can be accessed by Spark jobs running inside Docker containers. It supports:
    *   - The main e2e test resources directory
    *   - Any subdirectories that might contain test-specific resources
    *
    * The mounts are read-only for security and to prevent accidental modifications.
    *
    * @return
    *   Sequence of Docker volume mount arguments (-v flags)
    */
  private def buildTestResourceVolumeMounts(): Seq[String] = {
    val userDir          = System.getProperty("user.dir")
    val testResourcesDir = new File(s"$userDir/src/test/resources")

    if (!testResourcesDir.exists() || !testResourcesDir.isDirectory) {
      // No test resources directory, return empty
      return Seq.empty
    }

    // Mount patterns for different test resource locations
    // Add new patterns here if you have test resources in other locations
    val mountPatterns = Seq(
      // Main e2e resources directory (includes templates, configs, etc.)
      "e2e" -> "/opt/spark/work-dir/src/test/resources/e2e"
    )

    // Build volume mount arguments for existing directories
    mountPatterns.flatMap { case (subDir, containerPath) =>
      val hostDir = new File(testResourcesDir, subDir)
      if (hostDir.exists() && hostDir.isDirectory) {
        // Mount as read-only for security
        Seq("-v", s"${hostDir.getAbsolutePath}:$containerPath:ro")
      } else {
        Seq.empty
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
