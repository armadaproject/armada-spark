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

  // ========================================================================
  // Base helper method
  // ========================================================================

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

  /** Base builder for basic SparkPi job tests */
  private def baseSparkPiTest(testName: String, deployMode: String): E2ETestBuilder = {
    E2ETestBuilder(testName)
      .withBaseConfig(baseConfig)
      .withDeployMode(deployMode)
  }

  // ========================================================================
  // Gang Scheduling Tests
  // ========================================================================

  private def baseSparkPiGangTest(
      testName: String,
      deployMode: String,
      executorCount: Int,
      labels: Map[String, String]
  ): E2ETestBuilder = {
    baseSparkPiTest(testName, deployMode)
      .withGangJob("armada-spark")
      .withExecutors(executorCount)
      .withPodLabels(labels)
      .assertExecutorCount(executorCount)
  }

  test("Basic SparkPi job with gang scheduling - staticCluster", E2ETest) {
    baseSparkPiGangTest("basic-spark-pi-gang", "cluster", 3, Map("test-type" -> "basic"))
      .assertDriverExists()
      .assertPodLabels(Map("test-type" -> "basic"))
      .assertGangJob("armada-spark", 4) // 1 driver + 3 executors
      .run()
  }

  test("Basic SparkPi job with gang scheduling - staticClient", E2ETest) {
    baseSparkPiGangTest("basic-spark-pi-client", "client", 2, Map("test-type" -> "client-mode"))
      .assertExecutorGangJob("armada-spark", 2) // Only 2 executors, no driver
      .run()
  }

  // ========================================================================
  // Node Selector Tests
  // ========================================================================

  private def baseNodeSelectorTest(
      testName: String,
      deployMode: String,
      executorCount: Int,
      labels: Map[String, String]
  ): E2ETestBuilder = {
    baseSparkPiTest(testName, deployMode)
      .withNodeSelectors(Map("kubernetes.io/hostname" -> "armada-worker"))
      .withExecutors(executorCount)
      .withPodLabels(labels)
      .assertExecutorCount(executorCount)
      .assertNodeSelectors(Map("kubernetes.io/hostname" -> "armada-worker"))
  }

  test("SparkPi job with node selectors - staticCluster", E2ETest) {
    baseNodeSelectorTest(
      "spark-pi-node-selectors",
      "cluster",
      2,
      Map("test-type" -> "node-selector")
    )
      .assertDriverExists()
      .assertPodLabels(Map("test-type" -> "node-selector"))
      .run()
  }

  test("SparkPi job with node selectors - staticClient", E2ETest) {
    baseNodeSelectorTest(
      "spark-pi-node-selectors-client",
      "client",
      2,
      Map("test-type" -> "node-selector-client")
    )
      .assertExecutorsHaveLabels(Map("test-type" -> "node-selector-client"))
      .run()
  }

  // ========================================================================
  // Python Tests
  // ========================================================================

  private def basePythonSparkPiTest(
      testName: String,
      deployMode: String,
      executorCount: Int
  ): E2ETestBuilder = {
    baseSparkPiTest(testName, deployMode)
      .withPythonScript("/opt/spark/examples/src/main/python/pi.py")
      .withSparkConf(
        Map(
          "spark.kubernetes.file.upload.path"          -> "/tmp",
          "spark.kubernetes.executor.disableConfigMap" -> "true"
        )
      )
      .withExecutors(executorCount)
      .assertExecutorCount(executorCount)
  }

  test("Basic python SparkPi job - staticCluster", E2ETest) {
    basePythonSparkPiTest("python-spark-pi", "cluster", 2)
      .assertDriverExists()
      .run()
  }

  test("Basic python SparkPi job - staticClient", E2ETest) {
    basePythonSparkPiTest("python-spark-pi-client", "client", 2)
      .run()
  }

  // ========================================================================
  // Template Tests
  // ========================================================================

  private def baseTemplateTest(
      testName: String,
      deployMode: String,
      executorCount: Int,
      labels: Map[String, String]
  ): E2ETestBuilder = {
    baseSparkPiTest(testName, deployMode)
      .withJobTemplate(templatePath("spark-pi-job-template.yaml"))
      .withSparkConf(
        Map(
          "spark.armada.driver.jobItemTemplate"   -> templatePath("spark-pi-driver-template.yaml"),
          "spark.armada.executor.jobItemTemplate" -> templatePath("spark-pi-executor-template.yaml")
        )
      )
      .withExecutors(executorCount)
      .withPodLabels(labels)
      .assertExecutorCount(executorCount)
      // Assert template-specific labels from executor template (common to both modes)
      .assertExecutorsHaveLabels(
        Map(
          "app"             -> "spark-pi",
          "component"       -> "executor",
          "template-source" -> "e2e-test"
        )
      )
      // Assert template-specific annotations from executor template (common to both modes)
      .assertExecutorsHaveAnnotations(
        Map(
          "armada/component" -> "spark-executor",
          "armada/template"  -> "spark-pi-executor"
        )
      )
  }

  test("SparkPi job using job templates - staticCluster", E2ETest) {
    baseTemplateTest("spark-pi-templates", "cluster", 2, Map("test-type" -> "template"))
      .assertDriverExists()
      .assertPodLabels(Map("test-type" -> "template"))
      // Assert template-specific labels from driver template
      .assertDriverHasLabels(
        Map(
          "app"             -> "spark-pi",
          "component"       -> "driver",
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
      .run()
  }

  test("SparkPi job using job templates - staticClient", E2ETest) {
    baseTemplateTest(
      "spark-pi-templates-client",
      "client",
      2,
      Map("test-type" -> "template-client")
    )
      .assertExecutorsHaveLabels(Map("test-type" -> "template-client"))
      .run()
  }

  // ========================================================================
  // Feature Step Tests
  // ========================================================================

  private def baseFeatureStepTest(
      testName: String,
      deployMode: String,
      executorCount: Int,
      labels: Map[String, String]
  ): E2ETestBuilder = {
    val featureSteps = if (deployMode == "cluster") {
      Map(
        "spark.kubernetes.driver.pod.featureSteps" ->
          "org.apache.spark.deploy.armada.e2e.featurestep.DriverFeatureStep",
        "spark.kubernetes.executor.pod.featureSteps" ->
          "org.apache.spark.deploy.armada.e2e.featurestep.ExecutorFeatureStep"
      )
    } else {
      Map(
        "spark.kubernetes.executor.pod.featureSteps" ->
          "org.apache.spark.deploy.armada.e2e.featurestep.ExecutorFeatureStep"
      )
    }
    baseSparkPiTest(testName, deployMode)
      .withSparkConf(featureSteps)
      .withExecutors(executorCount)
      .withPodLabels(labels)
      .assertExecutorCount(executorCount)
      // Assert executor feature step labels (common to both modes)
      .assertExecutorsHaveLabels(
        Map(
          "feature-step"      -> "executor-applied",
          "feature-step-role" -> "executor"
        )
      )
      // Assert executor feature step annotation (common to both modes)
      .assertExecutorsHaveAnnotation("executor-feature-step", "configured")
      // Assert executor pod assertion for activeDeadlineSeconds (common to both modes)
      .withExecutorPodAssertion { pod =>
        Option(pod.getSpec.getActiveDeadlineSeconds).map(_.longValue).contains(1800L)
      }
  }

  test("SparkPi job with custom feature steps - staticCluster", E2ETest) {
    baseFeatureStepTest("spark-pi-feature-steps", "cluster", 2, Map("test-type" -> "feature-step"))
      .assertDriverExists()
      // Assert driver feature step labels (cluster-specific)
      .assertDriverHasLabels(
        Map(
          "feature-step"      -> "driver-applied",
          "feature-step-role" -> "driver"
        )
      )
      // Assert driver feature step annotation (cluster-specific)
      .assertDriverHasAnnotation("driver-feature-step", "configured")
      // Assert driver pod assertion for activeDeadlineSeconds (cluster-specific)
      .withDriverPodAssertion { pod =>
        Option(pod.getSpec.getActiveDeadlineSeconds).map(_.longValue).contains(3600L)
      }
      .run()
  }

  test("SparkPi job with custom feature steps - staticClient", E2ETest) {
    baseFeatureStepTest(
      "spark-pi-feature-steps-client",
      "client",
      2,
      Map("test-type" -> "feature-step-client")
    )
      .assertExecutorsHaveLabels(Map("test-type" -> "feature-step-client"))
      .run()
  }

  // ========================================================================
  // Ingress Tests
  // ========================================================================

  private def baseIngressTest(
      testName: String,
      executorCount: Int,
      labels: Map[String, String],
      ingressAnnotations: Map[String, String]
  )(
      configureIngress: E2ETestBuilder => E2ETestBuilder
  ): E2ETestBuilder = {
    configureIngress(baseSparkPiTest(testName, "cluster"))
      .withExecutors(executorCount)
      .withPodLabels(labels)
      .assertDriverExists()
      .assertExecutorCount(executorCount)
      .assertPodLabels(labels)
      .assertIngressAnnotations(ingressAnnotations)
  }

  private def baseIngressCLITest(
      testName: String,
      executorCount: Int,
      labels: Map[String, String],
      ingressAnnotations: Map[String, String]
  ): E2ETestBuilder = {
    baseIngressTest(testName, executorCount, labels, ingressAnnotations) { builder =>
      builder
        .withDriverIngress(ingressAnnotations)
        .withSparkConf("spark.armada.driver.ingress.tls.enabled", "false")
    }
  }

  private def baseIngressTemplateTest(
      testName: String,
      executorCount: Int,
      labels: Map[String, String],
      ingressAnnotations: Map[String, String]
  ): E2ETestBuilder = {
    baseIngressTest(testName, executorCount, labels, ingressAnnotations) { builder =>
      builder
        .withJobTemplate(templatePath("spark-pi-job-template.yaml"))
        .withSparkConf(
          Map(
            "spark.armada.driver.ingress.enabled" -> "true",
            "spark.armada.driver.jobItemTemplate" -> templatePath(
              "spark-pi-driver-ingress-template.yaml"
            ),
            "spark.armada.executor.jobItemTemplate" -> templatePath(
              "spark-pi-executor-template.yaml"
            )
          )
        )
    }
  }

  test("SparkPi job with driver ingress using cli - staticCluster", E2ETest) {
    baseIngressCLITest(
      "spark-pi-ingress",
      2,
      Map("test-type" -> "ingress"),
      Map(
        "nginx.ingress.kubernetes.io/rewrite-target"   -> "/",
        "nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP"
      )
    ).run()
  }

  test("SparkPi job with driver ingress using template - staticCluster", E2ETest) {
    baseIngressTemplateTest(
      "spark-pi-ingress-template",
      2,
      Map("test-type" -> "ingress-template"),
      Map(
        "nginx.ingress.kubernetes.io/rewrite-target"   -> "/",
        "nginx.ingress.kubernetes.io/backend-protocol" -> "HTTP",
        "kubernetes.io/ingress.class"                  -> "nginx"
      )
    ).run()
  }
}
