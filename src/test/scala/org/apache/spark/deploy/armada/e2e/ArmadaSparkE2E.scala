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
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Seconds, Span}

import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._
import TestConstants._

class ArmadaSparkE2E
    extends AnyFunSuite
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span(300, Seconds),
    interval = Span(2, Seconds)
  )

  private val baseQueueName = "e2e-template"

  private lazy val armadaClient = new ArmadaClient()
  private lazy val k8sClient    = new K8sClient()
  private lazy val orchestrator = new TestOrchestrator(armadaClient, k8sClient)

  private var baseConfig: TestConfig = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val props = loadProperties()

    // Get Scala binary version - either from system property or derive from full version
    // This should be "2.12" or "2.13", not the full version like "2.12.15"
    val scalaBinaryVersion = props.getProperty("scala.binary.version") match {
      case null | "" =>
        val fullVersion = props.getProperty("scala.version", "2.13.8")
        fullVersion.split('.').take(2).mkString(".")
      case binaryVersion => binaryVersion
    }

    val finalSparkVersion = props.getProperty("spark.version", "3.5.5")

    baseConfig = TestConfig(
      baseQueueName = props.getProperty("armada.queue", baseQueueName),
      imageName = props.getProperty("container.image", "spark:armada"),
      masterUrl = props.getProperty("armada.master", "armada://localhost:30002"),
      lookoutUrl = props.getProperty("armada.lookout.url", "http://localhost:30000"),
      scalaVersion = scalaBinaryVersion,
      sparkVersion = finalSparkVersion,
      sparkConfs = Map(
        "spark.armada.driver.labels"   -> "spark-role=driver",
        "spark.armada.executor.labels" -> "spark-role=executor"
      )
    )

    println(s"Test configuration loaded: $baseConfig")

    // Verify Armada cluster is ready before running tests
    val clusterReadyTimeout = ClusterReadyTimeout.toSeconds.toInt
    val testQueueName = s"${baseConfig.baseQueueName}-cluster-check-${System.currentTimeMillis()}"

    println(s"[CLUSTER-CHECK] Verifying Armada cluster readiness...")
    println(s"[CLUSTER-CHECK] Will retry for up to $clusterReadyTimeout seconds")

    val startTime                    = System.currentTimeMillis()
    var clusterReady                 = false
    var lastError: Option[Throwable] = None
    var attempts                     = 0

    while (!clusterReady && (System.currentTimeMillis() - startTime) < clusterReadyTimeout * 1000) {
      attempts += 1
      println(
        s"[CLUSTER-CHECK] Attempt #$attempts - Creating and verifying test queue: $testQueueName"
      )

      try {
        armadaClient.ensureQueueExists(testQueueName).futureValue
        println(s"[CLUSTER-CHECK] Queue creation and verification succeeded - cluster is ready!")
        clusterReady = true

        try {
          armadaClient.deleteQueue(testQueueName).futureValue
          println(s"[CLUSTER-CHECK] Test queue cleaned up")
        } catch {
          case _: Exception => // Ignore cleanup failures
        }
      } catch {
        case ex: Exception =>
          lastError = Some(ex)
          val elapsed = (System.currentTimeMillis() - startTime) / 1000
          println(s"[CLUSTER-CHECK] Attempt #$attempts failed after ${elapsed}s: ${ex.getMessage}")

          if ((System.currentTimeMillis() - startTime) < clusterReadyTimeout * 1000) {
            println(
              s"[CLUSTER-CHECK] Waiting ${ClusterCheckRetryDelay.toSeconds} seconds before retry..."
            )
            Thread.sleep(ClusterCheckRetryDelay.toMillis)
          }
      }
    }

    val totalTime = (System.currentTimeMillis() - startTime) / 1000
    if (!clusterReady) {
      throw new RuntimeException(
        s"Armada cluster not ready after $totalTime seconds ($attempts attempts). " +
          s"Last error: ${lastError.map(_.getMessage).getOrElse("Unknown")}"
      )
    }

    println(
      s"[CLUSTER-CHECK] Cluster verified ready after $totalTime seconds ($attempts attempts)"
    )
  }

  implicit val orch: TestOrchestrator = orchestrator

  test("Basic SparkPi job with gang scheduling", E2ETest) {
    E2ETestBuilder("basic-spark-pi-gang")
      .withBaseConfig(baseConfig)
      .withGangJob("kubernetes.io/hostname")
      .withExecutors(3)
      .withPodLabels(Map("test-type" -> "basic"))
      .assertDriverExists()
      .assertExecutorCount(3)
      .assertPodLabels(Map("test-type" -> "basic"))
      .assertGangJob("kubernetes.io/hostname", 4) // 1 driver + 3 executors
      .run()
  }

  test("SparkPi job with node selectors", E2ETest) {
    E2ETestBuilder("spark-pi-node-selectors")
      .withBaseConfig(baseConfig)
      .withPodLabels(Map("test-type" -> "node-selector"))
      .withNodeSelectors(Map("kubernetes.io/hostname" -> "armada-worker"))
      .assertDriverExists()
      .assertExecutorCount(2)
      .assertPodLabels(Map("test-type" -> "node-selector"))
      .assertNodeSelectors(Map("kubernetes.io/hostname" -> "armada-worker"))
      .run()
  }

  test("Basic python SparkPi job", E2ETest) {
    E2ETestBuilder("python-spark-pi")
      .withBaseConfig(baseConfig)
      .withPythonScript("/opt/spark/examples/src/main/python/pi.py")
      .withSparkConf(
        Map(
          "spark.kubernetes.file.upload.path"          -> "/tmp",
          "spark.kubernetes.executor.disableConfigMap" -> "true"
        )
      )
      .withExecutors(2)
      .assertDriverExists()
      .assertExecutorCount(2)
      .run()
  }

  test("SparkPi job using job templates", E2ETest) {
    E2ETestBuilder("spark-pi-templates")
      .withBaseConfig(baseConfig)
      .withJobTemplate(templatePath("spark-pi-job-template.yaml"))
      .withSparkConf(
        Map(
          "spark.armada.driver.jobItemTemplate"   -> templatePath("spark-pi-driver-template.yaml"),
          "spark.armada.executor.jobItemTemplate" -> templatePath("spark-pi-executor-template.yaml")
        )
      )
      .withPodLabels(Map("test-type" -> "template"))
      .assertDriverExists()
      .assertExecutorCount(2)
      .assertPodLabels(Map("test-type" -> "template"))
      // Assert template-specific labels from driver template
      .assertDriverHasLabels(
        Map(
          "app"             -> "spark-pi",
          "component"       -> "driver",
          "template-source" -> "e2e-test"
        )
      )
      // Assert template-specific labels from executor template
      .assertExecutorsHaveLabels(
        Map(
          "app"             -> "spark-pi",
          "component"       -> "executor",
          "template-source" -> "e2e-test"
        )
      )
      // Assert template-specific annotations from driver template
      .assertDriverHasAnnotations(
        Map(
          "armada/component" -> "spark-driver",
          "armada/template"  -> "spark-pi-driver"
        )
      )
      // Assert template-specific annotations from executor template
      .assertExecutorsHaveAnnotations(
        Map(
          "armada/component" -> "spark-executor",
          "armada/template"  -> "spark-pi-executor"
        )
      )
      .run()
  }

  test("SparkPi job with driver ingress using cli", E2ETest) {
    E2ETestBuilder("spark-pi-ingress")
      .withBaseConfig(baseConfig)
      .withDriverIngress(
        Map(
          "nginx.ingress.kubernetes.io/rewrite-target"   -> "/",
          "nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP"
        )
      )
      .withSparkConf("spark.armada.driver.ingress.tls.enabled", "false")
      .withPodLabels(Map("test-type" -> "ingress"))
      .assertDriverExists()
      .assertExecutorCount(2)
      .assertPodLabels(Map("test-type" -> "ingress"))
      .assertIngressAnnotations(
        Map(
          "nginx.ingress.kubernetes.io/rewrite-target"   -> "/",
          "nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP"
        )
      )
      .run()
  }

  test("SparkPi job with driver ingress using template", E2ETest) {
    E2ETestBuilder("spark-pi-ingress-template")
      .withBaseConfig(baseConfig)
      .withJobTemplate(templatePath("spark-pi-job-template.yaml"))
      .withSparkConf(
        Map(
          "spark.armada.driver.jobItemTemplate" -> templatePath(
            "spark-pi-driver-ingress-template.yaml"
          ),
          "spark.armada.executor.jobItemTemplate" -> templatePath("spark-pi-executor-template.yaml")
        )
      )
      .withPodLabels(Map("test-type" -> "ingress-template"))
      .assertDriverExists()
      .assertExecutorCount(2)
      .assertPodLabels(Map("test-type" -> "ingress-template"))
      .assertIngressAnnotations(
        Map(
          "nginx.ingress.kubernetes.io/rewrite-target"   -> "/",
          "nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP",
          "kubernetes.io/ingress.class"                  -> "nginx"
        )
      )
      .run()
  }

  test("SparkPi job with custom feature steps", E2ETest) {
    E2ETestBuilder("spark-pi-feature-steps")
      .withBaseConfig(baseConfig)
      .withSparkConf(
        Map(
          "spark.kubernetes.driver.pod.featureSteps" ->
            "org.apache.spark.deploy.armada.e2e.featurestep.DriverFeatureStep",
          "spark.kubernetes.executor.pod.featureSteps" ->
            "org.apache.spark.deploy.armada.e2e.featurestep.ExecutorFeatureStep"
        )
      )
      .withPodLabels(Map("test-type" -> "feature-step"))
      .assertDriverExists()
      .assertExecutorCount(2)
      .assertDriverHasLabels(
        Map(
          "feature-step"      -> "driver-applied",
          "feature-step-role" -> "driver"
        )
      )
      .assertDriverHasAnnotation("driver-feature-step", "configured")
      .assertExecutorsHaveLabels(
        Map(
          "feature-step"      -> "executor-applied",
          "feature-step-role" -> "executor"
        )
      )
      .assertExecutorsHaveAnnotation("executor-feature-step", "configured")
      .withDriverPodAssertion { pod =>
        // Assert active deadline is set
        Option(pod.getSpec.getActiveDeadlineSeconds).map(_.longValue) shouldBe Some(3600L)

        // Assert init container is present
        val initContainers =
          Option(pod.getSpec.getInitContainers).map(_.asScala).getOrElse(Seq.empty)
        val driverInitContainer = initContainers.find(_.getName == "driver-init")
        driverInitContainer should not be empty

        // Assert env var is present in main container
        val containers = Option(pod.getSpec.getContainers).map(_.asScala).getOrElse(Seq.empty)
        val envVars = containers.flatMap { container =>
          Option(container.getEnv).map(_.asScala).getOrElse(Seq.empty)
        }
        val driverFeatureEnv = envVars.find(_.getName == "DRIVER_FEATURE_STEP_ENV")
        driverFeatureEnv should not be empty
        driverFeatureEnv.get.getValue shouldBe "driver-feature-value"

        // Assert volume mount is present in main container
        val volumeMounts = containers.flatMap { container =>
          Option(container.getVolumeMounts).map(_.asScala).getOrElse(Seq.empty)
        }
        val driverFeatureMount = volumeMounts.find(_.getName == "driver-feature-volume")
        driverFeatureMount should not be empty
        driverFeatureMount.get.getMountPath shouldBe "/mnt/driver-feature"

        // Assert volume is present in pod spec
        val volumes             = Option(pod.getSpec.getVolumes).map(_.asScala).getOrElse(Seq.empty)
        val driverFeatureVolume = volumes.find(_.getName == "driver-feature-volume")
        driverFeatureVolume should not be empty
        driverFeatureVolume.get.getEmptyDir should not be null

        true
      }
      .withExecutorPodAssertion { pod =>
        Option(pod.getSpec.getActiveDeadlineSeconds).map(_.longValue) shouldBe Some(1800L)

        val initContainers =
          Option(pod.getSpec.getInitContainers).map(_.asScala).getOrElse(Seq.empty)
        val executorInitContainer = initContainers.find(_.getName == "executor-init")
        executorInitContainer should not be empty

        true
      }
      .run()
  }

  test("SparkPi job with feature steps and templates", E2ETest) {
    E2ETestBuilder("spark-pi-feature-steps-templates")
      .withBaseConfig(baseConfig)
      .withJobTemplate(templatePath("spark-pi-job-template.yaml"))
      .withSparkConf(
        Map(
          "spark.kubernetes.driver.pod.featureSteps" ->
            "org.apache.spark.deploy.armada.e2e.featurestep.DriverFeatureStep",
          "spark.kubernetes.executor.pod.featureSteps" ->
            "org.apache.spark.deploy.armada.e2e.featurestep.ExecutorFeatureStep",
          "spark.armada.driver.jobItemTemplate" -> templatePath(
            "spark-pi-driver-feature-step-template.yaml"
          ),
          "spark.armada.executor.jobItemTemplate" -> templatePath(
            "spark-pi-executor-feature-step-template.yaml"
          )
        )
      )
      .withPodLabels(Map("test-type" -> "feature-step-template"))
      .assertDriverExists()
      .assertExecutorCount(2)
      .withDriverPodAssertion { pod =>
        // Assert env var from feature step is present
        val containers = Option(pod.getSpec.getContainers).map(_.asScala).getOrElse(Seq.empty)
        val envVars = containers.flatMap { container =>
          Option(container.getEnv).map(_.asScala).getOrElse(Seq.empty)
        }
        val driverFeatureEnv = envVars.find(_.getName == "DRIVER_FEATURE_STEP_ENV")
        driverFeatureEnv should not be empty
        driverFeatureEnv.get.getValue shouldBe "driver-feature-value"

        // Assert volume from template is present
        val volumes             = Option(pod.getSpec.getVolumes).map(_.asScala).getOrElse(Seq.empty)
        val driverFeatureVolume = volumes.find(_.getName == "driver-feature-volume")
        driverFeatureVolume should not be empty
        driverFeatureVolume.get.getEmptyDir should not be null

        // Assert volume mount from feature step is present
        val volumeMounts = containers.flatMap { container =>
          Option(container.getVolumeMounts).map(_.asScala).getOrElse(Seq.empty)
        }
        val driverFeatureMount = volumeMounts.find(_.getName == "driver-feature-volume")
        driverFeatureMount should not be empty
        driverFeatureMount.get.getMountPath shouldBe "/mnt/driver-feature"

        // Assert template labels are present
        val metadata = pod.getMetadata
        metadata.getLabels.get("app") shouldBe "spark-pi"
        metadata.getLabels.get("component") shouldBe "driver"
        metadata.getLabels.get("template-source") shouldBe "e2e-test"

        // Assert feature step labels ARE also present (merged with template)
        metadata.getLabels.get("feature-step") shouldBe "driver-applied"
        metadata.getLabels.get("feature-step-role") shouldBe "driver"

        // Assert feature step annotation is present
        metadata.getAnnotations.get("driver-feature-step") shouldBe "configured"

        // Assert init container from feature step IS present (merged with template)
        val initContainers =
          Option(pod.getSpec.getInitContainers).map(_.asScala).getOrElse(Seq.empty)
        val driverInitContainer = initContainers.find(_.getName == "driver-init")
        driverInitContainer should not be empty

        true
      }
      .withExecutorPodAssertion { pod =>
        // Assert template labels are present
        val metadata = pod.getMetadata
        metadata.getLabels.get("app") shouldBe "spark-pi"
        metadata.getLabels.get("component") shouldBe "executor"

        // Assert feature step labels ARE also present (merged with template)
        metadata.getLabels.get("feature-step") shouldBe "executor-applied"
        metadata.getLabels.get("feature-step-role") shouldBe "executor"

        // Assert feature step annotation is present
        metadata.getAnnotations.get("executor-feature-step") shouldBe "configured"

        // Assert init container from feature step IS present (merged with template)
        val initContainers =
          Option(pod.getSpec.getInitContainers).map(_.asScala).getOrElse(Seq.empty)
        val executorInitContainer = initContainers.find(_.getName == "executor-init")
        executorInitContainer should not be empty

        true
      }
      .run()
  }

  private def loadProperties(): Properties = {
    val props = new Properties()

    // Check system properties first, then environment variables
    def getPropertyOrEnv(propName: String, envName: String): Option[String] = {
      sys.props.get(propName).orElse(sys.env.get(envName))
    }

    // Helper to set property if it exists in either system properties or environment variables
    def set(property: String, envvar: String): Unit = {
      getPropertyOrEnv(property, envvar).foreach(
        props.setProperty(property, _)
      )
    }

    // Set properties using the same precedence as the scripts
    set("container.image", "IMAGE_NAME")
    set("armada.master", "ARMADA_MASTER")
    set("armada.lookout.url", "ARMADA_LOOKOUT_URL")
    set("scala.version", "SCALA_VERSION")
    set("spark.version", "SPARK_VERSION")
    set("armada.queue", "ARMADA_QUEUE")

    // Also check for binary versions
    set("scala.binary.version", "SCALA_BIN_VERSION")
    set("spark.binary.version", "SPARK_BIN_VERSION")

    props
  }

  private def templatePath(name: String): String =
    s"src/test/resources/e2e/templates/$name"
}
