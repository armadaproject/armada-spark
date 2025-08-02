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

import java.io.{File, FileInputStream}
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
  private val configFile    = new File("src/test/resources/e2e/spark-pi-e2e.conf")

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
      sparkVersion = finalSparkVersion
    )

    println(s"Test configuration loaded: $baseConfig")

    // Verify Armada cluster is ready before running tests
    val clusterReadyTimeout = ClusterReadyTimeout.toSeconds.toInt
    val testQueueName = s"${baseConfig.baseQueueName}-cluster-check-${System.currentTimeMillis()}"

    println(s"[CLUSTER-CHECK] Verifying Armada cluster readiness...")
    println(s"[CLUSTER-CHECK] Will retry for up to ${clusterReadyTimeout} seconds")

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

    if (!clusterReady) {
      val totalTime = (System.currentTimeMillis() - startTime) / 1000
      throw new RuntimeException(
        s"Armada cluster not ready after ${totalTime} seconds (${attempts} attempts). " +
          s"Last error: ${lastError.map(_.getMessage).getOrElse("Unknown")}"
      )
    }

    val totalTime = (System.currentTimeMillis() - startTime) / 1000
    println(
      s"[CLUSTER-CHECK] Cluster verified ready after ${totalTime} seconds (${attempts} attempts)"
    )
  }

  test("Basic SparkPi job", E2ETest) {
    val config = baseConfig.copy(
      sparkConfs = Map("spark.armada.pod.labels" -> "test-type=basic")
    )

    val result = orchestrator.runTest("basic-spark-pi", config).futureValue
    result.status shouldBe JobSetStatus.Success
  }

  test("SparkPi job with node selectors", E2ETest) {
    val config = baseConfig.copy(
      sparkConfs = Map(
        "spark.armada.scheduling.nodeSelectors" -> "kubernetes.io/hostname=armada-worker",
        "spark.armada.pod.labels"               -> "test-type=node-selector"
      )
    )

    val result = orchestrator.runTest("spark-pi-node-selectors", config).futureValue
    result.status shouldBe JobSetStatus.Success
  }

  test("SparkPi job using job templates", E2ETest) {
    val config = baseConfig.copy(
      sparkConfs = Map(
        "spark.armada.jobTemplate"              -> templatePath("spark-pi-job-template.yaml"),
        "spark.armada.driver.jobItemTemplate"   -> templatePath("spark-pi-driver-template.yaml"),
        "spark.armada.executor.jobItemTemplate" -> templatePath("spark-pi-executor-template.yaml"),
        "spark.armada.pod.labels"               -> "test-type=template"
      )
    )

    val result = orchestrator.runTest("spark-pi-templates", config).futureValue
    result.status shouldBe JobSetStatus.Success
  }

  test("SparkPi job with driver ingress using cli", E2ETest) {
    val ingressAssertion = new IngressAssertion(
      requiredAnnotations = Set(
        "nginx.ingress.kubernetes.io/rewrite-target",
        "nginx.ingress.kubernetes.io/backend-protocol",
        "armada_jobset_id",
        "armada_owner"
      )
    )

    val config = baseConfig.copy(
      sparkConfs = Map(
        "spark.armada.driver.ingress.enabled" -> "true",
        "spark.armada.driver.ingress.annotations" ->
          "nginx.ingress.kubernetes.io/rewrite-target=/,nginx.ingress.kubernetes.io/backend-protocol=HTTP",
        "spark.armada.driver.ingress.tls.enabled" -> "false",
        "spark.armada.pod.labels"                 -> "test-type=ingress"
      ),
      assertions = Seq(ingressAssertion)
    )

    val result = orchestrator.runTest("spark-pi-ingress", config).futureValue
    result.status shouldBe JobSetStatus.Success
    AssertionRunner.assertAllSuccess(result.assertionResults)
  }

  test("SparkPi job with driver ingress using template", E2ETest) {
    val templateIngressAssertion = new TemplateIngressAssertion()

    val config = baseConfig.copy(
      sparkConfs = Map(
        "spark.armada.jobTemplate" -> templatePath("spark-pi-job-template.yaml"),
        "spark.armada.driver.jobItemTemplate" -> templatePath(
          "spark-pi-driver-ingress-template.yaml"
        ),
        "spark.armada.executor.jobItemTemplate" -> templatePath("spark-pi-executor-template.yaml"),
        "spark.armada.pod.labels"               -> "test-type=ingress-template"
      ),
      assertions = Seq(templateIngressAssertion)
    )

    val result = orchestrator.runTest("spark-pi-ingress-template", config).futureValue
    result.status shouldBe JobSetStatus.Success
    AssertionRunner.assertAllSuccess(result.assertionResults)
  }

  private def loadProperties(): Properties = {
    val props = new Properties()

    // Load from config file if it exists
    if (configFile.exists()) {
      val fis = new FileInputStream(configFile)
      try {
        props.load(fis)
      } finally {
        fis.close()
      }
    }

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
