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

package org.apache.spark.deploy.armada.submit

import api.submit.JobSubmitRequestItem
import k8s.io.api.core.v1.generated.{EnvVar, PodSpec, Volume, VolumeMount}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path}
import scala.concurrent.duration._

class ArmadaClientApplicationSuite extends AnyFunSuite with BeforeAndAfter with Matchers {

  private var sparkConf: SparkConf = _
  private val armadaClientApp      = new ArmadaClientApplication()
  private var tempDir: Path        = _

  before {
    tempDir = Files.createTempDirectory("armada-client-test-")
    sparkConf = new SparkConf()
      .set("spark.master", "armada://localhost:50051")
      .set("spark.app.name", "test-app")
      .set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
      .set(Config.ARMADA_JOB_SET_ID.key, "test-job-set")
      .set(Config.ARMADA_SPARK_JOB_NAMESPACE.key, "test-namespace")
      .set(Config.ARMADA_SPARK_JOB_PRIORITY.key, "1.0")
      .set(Config.CONTAINER_IMAGE.key, "spark:3.5.0")
      .set(Config.ARMADA_JOB_NODE_SELECTORS.key, "node-type=compute")
  }

  after {
    if (tempDir != null) {
      tempDir.toFile.listFiles().foreach(_.delete())
      Files.deleteIfExists(tempDir)
    }
  }

  private def createJobTemplateFile(queue: String, jobSetId: String): File = {
    val templateContent =
      s"""queue: $queue
         |jobSetId: $jobSetId
         |jobRequestItems:
         |  - priority: 1.5
         |    namespace: template-namespace
         |    labels:
         |      template-label: template-value
         |    annotations:
         |      template-annotation: template-value
         |""".stripMargin

    val file   = tempDir.resolve("job-template.yaml").toFile
    val writer = new FileWriter(file)
    try {
      writer.write(templateContent)
    } finally {
      writer.close()
    }
    file
  }

  private def createJobItemTemplateFile(
      priority: Double,
      namespace: String,
      filename: String = "job-item-template.yaml"
  ): File = {
    val templateContent =
      s"""priority: $priority
         |namespace: $namespace
         |labels:
         |  item-template-label: item-template-value
         |annotations:
         |  item-template-annotation: item-template-value
         |""".stripMargin

    val file   = tempDir.resolve(filename).toFile
    val writer = new FileWriter(file)
    try {
      writer.write(templateContent)
    } finally {
      writer.close()
    }
    file
  }

  test("validateArmadaJobConfig should create config without templates") {
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)

    config.cliConfig.queue shouldBe Some("test-queue")
    config.cliConfig.jobSetId shouldBe Some("test-job-set")
    config.cliConfig.namespace shouldBe Some("test-namespace")
    config.cliConfig.priority shouldBe Some(1.0)
    config.cliConfig.containerImage shouldBe Some("spark:3.5.0")
    config.jobTemplate shouldBe None
    config.driverJobItemTemplate shouldBe None
    config.executorJobItemTemplate shouldBe None
  }

  test("should use template values when CLI config not provided") {
    val templateFile = createJobTemplateFile("template-queue", "template-job-set")
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)

    sparkConf.remove(Config.ARMADA_JOB_QUEUE.key)
    val configWithTemplateQueue = armadaClientApp.validateArmadaJobConfig(sparkConf)
    configWithTemplateQueue.queue shouldBe "template-queue"
    configWithTemplateQueue.jobSetId shouldBe "test-job-set"

    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    val configWithTemplateJobSetId = armadaClientApp.validateArmadaJobConfig(sparkConf)
    configWithTemplateJobSetId.queue shouldBe "test-queue"
    configWithTemplateJobSetId.jobSetId shouldBe "template-job-set"
  }

  test("should use app ID fallback when no template and no CLI jobSetId") {
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    sparkConf.set("spark.app.id", "test-app-id")

    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    config.jobSetId shouldBe "test-app-id"
  }

  test("should validate required configuration values") {
    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "")
    val emptyQueueException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    emptyQueueException.getMessage should include("Queue name must be set")

    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
    sparkConf.set(Config.ARMADA_JOB_SET_ID.key, "")
    val emptyJobSetIdException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    emptyJobSetIdException.getMessage should include("Empty jobSetId is not allowed")

    sparkConf.set(Config.ARMADA_JOB_SET_ID.key, "test-job-set")
    sparkConf.remove(Config.CONTAINER_IMAGE.key)
    val missingImageException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    missingImageException.getMessage should include("Container image must be set")

    sparkConf.set(Config.CONTAINER_IMAGE.key, "")
    val emptyImageException = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    emptyImageException.getMessage should include("Empty container image is not allowed")
  }

  test("validateArmadaJobConfig should correctly parse gangUniformityLabel configuration") {
    sparkConf.remove(Config.ARMADA_JOB_NODE_SELECTORS.key)
    sparkConf.set(Config.ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY.key, "zone")

    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)

    config.cliConfig.nodeSelectors shouldBe Map.empty
    config.cliConfig.nodeUniformityLabel shouldBe Some("zone")
  }

  test("validateArmadaJobConfig should load all template types with correct precedence") {
    val jobTemplateFile = createJobTemplateFile("all-template-queue", "all-template-job-set")
    val driverTemplateFile =
      createJobItemTemplateFile(1.5, "all-driver-namespace", "driver-template.yaml")
    val executorTemplateFile =
      createJobItemTemplateFile(2.5, "all-executor-namespace", "executor-template.yaml")

    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, jobTemplateFile.getAbsolutePath)
    sparkConf.set(Config.ARMADA_DRIVER_JOB_ITEM_TEMPLATE.key, driverTemplateFile.getAbsolutePath)
    sparkConf.set(
      Config.ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE.key,
      executorTemplateFile.getAbsolutePath
    )

    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)

    config.jobTemplate should not be empty
    config.jobTemplate.get.queue shouldBe "all-template-queue"
    config.driverJobItemTemplate should not be empty
    config.driverJobItemTemplate.get.namespace shouldBe "all-driver-namespace"
    config.driverJobItemTemplate.get.priority shouldBe 1.5
    config.driverJobItemTemplate.get.labels should contain(
      "item-template-label" -> "item-template-value"
    )
    config.executorJobItemTemplate should not be empty
    config.executorJobItemTemplate.get.namespace shouldBe "all-executor-namespace"
    config.executorJobItemTemplate.get.priority shouldBe 2.5
    config.executorJobItemTemplate.get.labels should contain(
      "item-template-label" -> "item-template-value"
    )
    config.cliConfig.queue shouldBe Some("test-queue")
    config.cliConfig.jobSetId shouldBe Some("test-job-set")
  }
  test("mergeExecutorTemplate should merge template with runtime configuration") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = 0.5,
      namespace = "template-namespace",
      annotations = Map("template-annotation" -> "template-value"),
      labels = Map("template-label" -> "template-value"),
      podSpec = Some(
        PodSpec()
          .withTolerations(
            Seq(
              k8s.io.api.core.v1.generated
                .Toleration()
                .withKey("gpu")
                .withOperator("Equal")
                .withValue("true")
                .withEffect("NoSchedule")
            )
          )
          .withHostAliases(
            Seq(
              k8s.io.api.core.v1.generated
                .HostAlias()
                .withIp("10.0.0.1")
                .withHostnames(Seq("custom-host"))
            )
          )
          .withDnsPolicy("ClusterFirst")
          .withNodeSelector(Map("template-node-type" -> "gpu-enabled", "zone" -> "us-west"))
          .withAffinity(
            k8s.io.api.core.v1.generated
              .Affinity()
              .withNodeAffinity(
                k8s.io.api.core.v1.generated
                  .NodeAffinity()
                  .withRequiredDuringSchedulingIgnoredDuringExecution(
                    k8s.io.api.core.v1.generated
                      .NodeSelector()
                      .withNodeSelectorTerms(
                        Seq(
                          k8s.io.api.core.v1.generated
                            .NodeSelectorTerm()
                            .withMatchExpressions(
                              Seq(
                                k8s.io.api.core.v1.generated
                                  .NodeSelectorRequirement()
                                  .withKey("kubernetes.io/arch")
                                  .withOperator("In")
                                  .withValues(Seq("amd64"))
                              )
                            )
                        )
                      )
                  )
              )
          )
          .withVolumes(
            Seq(
              k8s.io.api.core.v1.generated
                .Volume()
                .withName("template-volume")
            )
          )
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("template-container")
                .withImage("executor-template-image:1.0")
                .withVolumeMounts(
                  Seq(
                    k8s.io.api.core.v1.generated
                      .VolumeMount()
                      .withName("template-volume")
                      .withMountPath("/tmp/template-data")
                  )
                )
            )
          )
      )
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(3.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map("node-type" -> "compute"),
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = Some(template),
      cliConfig = cliConfig
    )

    val javaOptEnvVars = Seq(EnvVar().withName("SPARK_JAVA_OPT_0").withValue("-Xmx1g"))

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = 3.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      annotations =
        Map("runtime-annotation" -> "runtime-value", "template-annotation" -> "template-value"),
      labels = Map("runtime-label" -> "runtime-value", "template-label" -> "template-value"),
      nodeSelectors = Map("node-type" -> "compute"),
      driverResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
      executorResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      Some(template),
      0,
      resolvedConfig,
      armadaJobConfig,
      javaOptEnvVars,
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.priority shouldBe 3.0
    result.namespace shouldBe "runtime-namespace"
    result.annotations should contain("template-annotation" -> "template-value")
    result.annotations should contain("runtime-annotation" -> "runtime-value")
    result.labels should contain("template-label" -> "template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(0)
    podSpec.nodeSelector shouldBe Map("node-type" -> "compute")

    podSpec.initContainers should have size 1
    val initContainer = podSpec.initContainers.head
    initContainer.name shouldBe Some("init")
    initContainer.image shouldBe Some("busybox")
    initContainer.command.take(2) shouldBe Seq("sh", "-c")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some("executor")
    container.image shouldBe Some("spark:3.5.0")
    container.command shouldBe Seq("/opt/entrypoint.sh")
    container.args shouldBe Seq("executor")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain key "SPARK_EXECUTOR_ID"
    envVars should contain(
      "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
    )

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"

    podSpec.tolerations should have size 1
    val toleration = podSpec.tolerations.head
    toleration.key shouldBe Some("gpu")
    toleration.operator shouldBe Some("Equal")
    toleration.value shouldBe Some("true")
    toleration.effect shouldBe Some("NoSchedule")

    podSpec.hostAliases should have size 1
    val hostAlias = podSpec.hostAliases.head
    hostAlias.ip shouldBe Some("10.0.0.1")
    hostAlias.hostnames shouldBe Seq("custom-host")

    podSpec.dnsPolicy shouldBe Some("ClusterFirst")

    podSpec.nodeSelector shouldBe Map("node-type" -> "compute")
    podSpec.nodeSelector should not contain ("template-node-type" -> "gpu-enabled")
    podSpec.nodeSelector should not contain ("zone"               -> "us-west")

    podSpec.affinity should not be empty
    val affinity = podSpec.affinity.get
    affinity.nodeAffinity should not be empty
    val nodeAffinity = affinity.nodeAffinity.get
    nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution should not be empty

    podSpec.volumes should not be empty
    val volumeNames = podSpec.volumes.map(_.name.getOrElse("")).toSet
    volumeNames should contain("template-volume")

    val containerWithMounts = podSpec.containers.find(_.volumeMounts.nonEmpty)
    containerWithMounts should not be empty
    val volumeMounts = containerWithMounts.get.volumeMounts
    val mountNames   = volumeMounts.map(_.name.getOrElse("")).toSet
    mountNames should contain("template-volume")
  }

  test("mergeExecutorTemplate should handle template without podSpec") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = None
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(2.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = Some(template),
      cliConfig = cliConfig
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = 2.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      annotations = Map.empty,
      labels = Map.empty,
      nodeSelectors = Map.empty,
      driverResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi")),
      executorResources =
        armadaClientApp.ResolvedResourceConfig(Some("1"), Some("1"), Some("1Gi"), Some("1Gi"))
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      Some(template),
      1,
      resolvedConfig,
      armadaJobConfig,
      Seq.empty[EnvVar],
      "driver-service",
      7078,
      Seq.empty[Volume],
      sparkConf
    )

    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(0)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.initContainers should have size 1
    val initContainer = podSpec.initContainers.head
    initContainer.name shouldBe Some("init")
    initContainer.image shouldBe Some("busybox")
    initContainer.command.take(2) shouldBe Seq("sh", "-c")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some("executor")
    container.image shouldBe Some("spark:3.5.0")
    container.command shouldBe Seq("/opt/entrypoint.sh")
    container.args shouldBe Seq("executor")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain key "SPARK_EXECUTOR_ID"
    envVars should contain(
      "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
    )

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"
  }

  test("validateRequiredConfig should validate all requirements") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(1.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    armadaClientApp.validateRequiredConfig(cliConfig, None, None, None, sparkConf)

    val invalidConfig = cliConfig.copy(containerImage = None)
    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(invalidConfig, None, None, None, sparkConf)
    }
    exception.getMessage should include("Container image must be set")

    val emptyImageConfig = cliConfig.copy(containerImage = Some(""))
    val emptyException = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(emptyImageConfig, None, None, None, sparkConf)
    }
    emptyException.getMessage should include("Empty container image is not allowed")
  }

  test("resolveValue should follow precedence correctly") {
    armadaClientApp.resolveValue(Some("cli"), Some("template"), "default") shouldBe "cli"
    armadaClientApp.resolveValue(None, Some("template"), "default") shouldBe "template"
    armadaClientApp.resolveValue(None, None, "default") shouldBe "default"

    armadaClientApp.resolveValue(Some(1.0), Some(2.0), 3.0) shouldBe 1.0
    armadaClientApp.resolveValue(None, Some(2.0), 3.0) shouldBe 2.0
    armadaClientApp.resolveValue(None, None, 3.0) shouldBe 3.0
  }

  test("mergeDriverTemplate should merge template with runtime configuration") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = 0.5,
      namespace = "template-namespace",
      annotations = Map("template-annotation" -> "template-value"),
      labels = Map("template-label" -> "template-value"),
      podSpec = Some(
        PodSpec()
          .withTolerations(
            Seq(
              k8s.io.api.core.v1.generated
                .Toleration()
                .withKey("dedicated")
                .withOperator("Equal")
                .withValue("spark-driver")
                .withEffect("NoSchedule")
            )
          )
          .withActiveDeadlineSeconds(3600)
          .withPriorityClassName("high-priority")
          .withNodeSelector(Map("driver-node-type" -> "memory-optimized", "tier" -> "production"))
          .withVolumes(
            Seq(
              k8s.io.api.core.v1.generated
                .Volume()
                .withName("driver-template-volume")
            )
          )
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("driver-template-container")
                .withImage("driver-template-image:1.0")
                .withVolumeMounts(
                  Seq(
                    k8s.io.api.core.v1.generated
                      .VolumeMount()
                      .withName("driver-template-volume")
                      .withMountPath("/driver/template-data")
                  )
                )
            )
          )
      )
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = 3.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      annotations =
        Map("runtime-annotation" -> "runtime-value", "template-annotation" -> "template-value"),
      labels = Map("runtime-label" -> "runtime-value", "template-label" -> "template-value"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(3.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = Some(template),
      executorJobItemTemplate = None,
      cliConfig = cliConfig
    )

    val result = armadaClientApp.mergeDriverTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      7078,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Seq("--arg1", "--arg2"),
      sparkConf
    )

    result.priority shouldBe 3.0
    result.namespace shouldBe "runtime-namespace"
    result.annotations should contain("template-annotation" -> "template-value")
    result.annotations should contain("runtime-annotation" -> "runtime-value")
    result.labels should contain("template-label" -> "template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(0)
    podSpec.nodeSelector shouldBe Map(
      "driver-node-type" -> "memory-optimized",
      "tier"             -> "production"
    )

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some("driver")
    container.image shouldBe Some("spark:3.5.0")
    container.command shouldBe Seq("/opt/entrypoint.sh")
    container.args should contain("driver")
    container.args should contain("--class")
    container.args should contain("org.example.TestClass")
    container.args should contain allOf ("--arg1", "--arg2")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    container.ports should not be empty
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(7078)

    result.services should have size 1
    val service = result.services.head
    service.ports should contain(7078)

    podSpec.tolerations should have size 1
    val toleration = podSpec.tolerations.head
    toleration.key shouldBe Some("dedicated")
    toleration.operator shouldBe Some("Equal")
    toleration.value shouldBe Some("spark-driver")
    toleration.effect shouldBe Some("NoSchedule")

    podSpec.activeDeadlineSeconds shouldBe Some(3600)
    podSpec.priorityClassName shouldBe Some("high-priority")

    podSpec.nodeSelector shouldBe Map(
      "driver-node-type" -> "memory-optimized",
      "tier"             -> "production"
    )

    podSpec.volumes should not be empty
    val volumeNames = podSpec.volumes.map(_.name.getOrElse("")).toSet
    volumeNames should contain("driver-template-volume")

    val containerWithMounts = podSpec.containers.find(_.volumeMounts.nonEmpty)
    containerWithMounts should not be empty
    val volumeMounts = containerWithMounts.get.volumeMounts
    val mountNames   = volumeMounts.map(_.name.getOrElse("")).toSet
    mountNames should contain("driver-template-volume")
  }

  test("mergeDriverTemplate should handle template without podSpec") {
    val template: JobSubmitRequestItem = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = None
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "runtime-namespace",
      priority = 2.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      annotations = Map.empty,
      labels = Map.empty,
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(2.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = Some(template),
      executorJobItemTemplate = None,
      cliConfig = cliConfig
    )

    val result = armadaClientApp.mergeDriverTemplate(
      Some(template),
      resolvedConfig,
      armadaJobConfig,
      7078,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Seq.empty[String],
      sparkConf
    )

    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(0)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some("driver")
    container.image shouldBe Some("spark:3.5.0")
    container.command shouldBe Seq("/opt/entrypoint.sh")
    container.args should contain("driver")
    container.args should contain("--class")
    container.args should contain("org.example.TestClass")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    container.ports should not be empty
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(7078)

    result.services should have size 1
    val service = result.services.head
    service.ports should contain(7078)
  }

  test("resolveConfig should resolve values with correct precedence") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("cli-namespace"),
      priority = Some(2.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://cli-url:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(120.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val result = armadaClientApp.resolveJobConfig(
      cliConfig,
      None,
      Map.empty,
      Map.empty,
      sparkConf
    )

    result.namespace shouldBe "cli-namespace"
    result.priority shouldBe 2.0
    result.containerImage shouldBe "spark:3.5.0"
    result.armadaClusterUrl shouldBe "armada://cli-url:50051"
    result.executorConnectionTimeout shouldBe 120.seconds
  }

  test("resolveJobConfig should merge template and runtime values") {
    val template = JobSubmitRequestItem(
      priority = 1.5,
      namespace = "template-namespace",
      annotations = Map("template-key" -> "template-value"),
      labels = Map("template-label" -> "template-value")
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("runtime-namespace"),
      priority = Some(3.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig
    )

    val runtimeAnnotations = Map("runtime-key" -> "runtime-value")
    val runtimeLabels      = Map("runtime-label" -> "runtime-value")

    val result = armadaClientApp.resolveJobConfig(
      armadaJobConfig.cliConfig,
      Some(template),
      runtimeAnnotations,
      runtimeLabels,
      sparkConf
    )

    result.priority shouldBe 3.0
    result.namespace shouldBe "runtime-namespace"
    result.annotations should contain("template-key" -> "template-value")
    result.annotations should contain("runtime-key" -> "runtime-value")
    result.labels should contain("template-label" -> "template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
  }

  test("mergeDriverTemplate should create valid driver job specification when no template provided") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(1.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map("node-type" -> "compute"),
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(
        limitCores = Some("2"),
        requestCores = Some("1"),
        limitMemory = Some("2Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None, // No template provided
      executorJobItemTemplate = None,
      cliConfig = cliConfig
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = 1.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "spark://driver:7077",
      executorConnectionTimeout = 300.seconds,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "driver"),
      nodeSelectors = Map("node-type" -> "compute"),
      driverResources = armadaClientApp.ResolvedResourceConfig(
        limitCores = Some("2"),
        requestCores = Some("1"),
        limitMemory = Some("2Gi"),
        requestMemory = Some("1Gi")
      ),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val result = armadaClientApp.mergeDriverTemplate(
      template = None, // No template - will create blank one internally
      resolvedConfig = resolvedConfig,
      armadaJobConfig = armadaJobConfig,
      driverPort = 7078,
      mainClass = "org.example.SparkApp",
      volumes = Seq.empty,
      volumeMounts = Seq.empty,
      additionalDriverArgs = Seq("--arg1", "value1"),
      conf = sparkConf
    )

    result.priority shouldBe 1.0
    result.namespace shouldBe "test-namespace"
    result.annotations should contain("app" -> "spark-test")
    result.labels should contain("component" -> "driver")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(0)
    podSpec.nodeSelector shouldBe Map("node-type" -> "compute")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some("driver")
    container.image shouldBe Some("spark:3.5.0")
    container.command shouldBe Seq("/opt/entrypoint.sh")
    container.args should contain("driver")
    container.args should contain("--class")
    container.args should contain("org.example.SparkApp")
    container.args should contain allOf ("--arg1", "value1")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    container.ports should not be empty
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(7078)

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"

    result.services should have size 1
    result.services.head.ports should contain(7078)
  }

  test("mergeExecutorTemplate should create valid executor job specification when no template provided") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(1.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None, // No template provided
      cliConfig = cliConfig
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = 1.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "executor"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(
        limitCores = Some("1"),
        requestCores = Some("1"),
        limitMemory = Some("1Gi"),
        requestMemory = Some("1Gi")
      )
    )

    val result = armadaClientApp.mergeExecutorTemplate(
      template = None, // No template - will create blank one internally
      index = 1,
      resolvedConfig = resolvedConfig,
      armadaJobConfig = armadaJobConfig,
      javaOptEnvVars = Seq(EnvVar().withName("SPARK_JAVA_OPT_0").withValue("-Xmx1g")),
      driverHostname = "driver-service",
      driverPort = 7078,
      volumes = Seq.empty,
      conf = sparkConf
    )

    result.priority shouldBe 1.0
    result.namespace shouldBe "test-namespace"
    result.annotations should contain("app" -> "spark-test")
    result.labels should contain("component" -> "executor")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(0)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.initContainers should have size 1
    val initContainer = podSpec.initContainers.head
    initContainer.name shouldBe Some("init")
    initContainer.image shouldBe Some("busybox")
    initContainer.command.take(2) shouldBe Seq("sh", "-c")

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some("executor")
    container.image shouldBe Some("spark:3.5.0")
    container.command shouldBe Seq("/opt/entrypoint.sh")
    container.args shouldBe Seq("executor")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain key "SPARK_EXECUTOR_ID"
    envVars should contain(
      "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
    )
    envVars should contain("SPARK_JAVA_OPT_0" -> "-Xmx1g")

    container.resources should not be empty
    val resources = container.resources.get
    resources.limits should contain key "memory"
    resources.limits should contain key "cpu"
    resources.requests should contain key "memory"
    resources.requests should contain key "cpu"
  }

  test("createDriverJob should create driver job without templates") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(1.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = 1.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "driver"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig
    )

    val clientArguments = ClientArguments(
      mainAppResource = JavaMainAppResource(Some("app.jar")),
      mainClass = "org.example.SparkApp",
      driverArgs = Array("--input", "data.txt"),
      proxyUser = None
    )

    val configGenerator = new ConfigGenerator(tempDir.toString, sparkConf)

    val result = armadaClientApp.createDriverJob(
      armadaJobConfig = armadaJobConfig,
      resolvedConfig = resolvedConfig,
      configGenerator = configGenerator,
      clientArguments = clientArguments,
      primaryResource = Seq("app.jar"),
      confSeq = Seq("--conf", "spark.executor.memory=1g"),
      conf = sparkConf
    )

    result.priority shouldBe 1.0
    result.namespace shouldBe "test-namespace"
    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(0)
    podSpec.nodeSelector shouldBe Map.empty[String, String]

    podSpec.containers should have size 1
    val container = podSpec.containers.head
    container.name shouldBe Some("driver")
    container.image shouldBe Some("spark:3.5.0")
    container.command shouldBe Seq("/opt/entrypoint.sh")

    container.env should not be empty
    val envVars = container.env
      .filter(e => e.name.isDefined && e.value.isDefined)
      .map(e => e.name.get -> e.value.get)
      .toMap
    envVars should contain("SPARK_CONF_DIR" -> "/opt/spark/conf")

    container.ports should not be empty
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(7078)

    result.services should have size 1
  }

  test("createExecutorJobs should create multiple executor jobs") {
    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(1.0),
      containerImage = Some("spark:3.5.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val resolvedConfig = armadaClientApp.ResolvedJobConfig(
      namespace = "test-namespace",
      priority = 1.0,
      containerImage = "spark:3.5.0",
      armadaClusterUrl = "armada://localhost:50051",
      executorConnectionTimeout = 300.seconds,
      annotations = Map("app" -> "spark-test"),
      labels = Map("component" -> "executor"),
      nodeSelectors = Map.empty,
      driverResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResolvedResourceConfig(None, None, None, None)
    )

    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = cliConfig
    )

    val configGenerator = new ConfigGenerator(tempDir.toString, sparkConf)

    val results = armadaClientApp.createExecutorJobs(
      armadaJobConfig = armadaJobConfig,
      resolvedConfig = resolvedConfig,
      configGenerator = configGenerator,
      driverHostname = "driver-service",
      executorCount = 2,
      conf = sparkConf
    )

    results should have size 2
    results.zipWithIndex.foreach { case (job, _) =>
      job.priority shouldBe 1.0
      job.namespace shouldBe "test-namespace"
      job.podSpec should not be empty
      val podSpec = job.podSpec.get

      podSpec.restartPolicy shouldBe Some("Never")
      podSpec.terminationGracePeriodSeconds shouldBe Some(0)
      podSpec.nodeSelector shouldBe Map.empty[String, String]

      podSpec.initContainers should have size 1
      val initContainer = podSpec.initContainers.head
      initContainer.name shouldBe Some("init")
      initContainer.image shouldBe Some("busybox")
      initContainer.command.take(2) shouldBe Seq("sh", "-c")

      podSpec.containers should have size 1
      val container = podSpec.containers.head
      container.name shouldBe Some("executor")
      container.image shouldBe Some("spark:3.5.0")
      container.command shouldBe Seq("/opt/entrypoint.sh")
      container.args shouldBe Seq("executor")

      val envVars = container.env
        .filter(e => e.name.isDefined && e.value.isDefined)
        .map(e => e.name.get -> e.value.get)
        .toMap
      envVars should contain key "SPARK_EXECUTOR_ID"
      envVars should contain(
        "SPARK_DRIVER_URL" -> "spark://CoarseGrainedScheduler@driver-service:7078"
      )
    }
  }

  test("submitArmadaJob should validate executor count is greater than zero") {
    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = None,
      cliConfig = armadaClientApp.CLIConfig(
        queue = Some("test-queue"),
        jobSetId = Some("test-job-set"),
        namespace = Some("test-namespace"),
        priority = Some(1.0),
        containerImage = Some("spark:3.5.0"),
        podLabels = Map.empty,
        driverLabels = Map.empty,
        executorLabels = Map.empty,
        armadaClusterUrl = Some("armada://localhost:50051"),
        nodeSelectors = Map.empty,
        nodeUniformityLabel = None,
        executorConnectionTimeout = Some(300.seconds),
        driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
        executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
      )
    )

    val clientArguments = ClientArguments(
      mainAppResource = JavaMainAppResource(Some("app.jar")),
      mainClass = "org.example.SparkApp",
      driverArgs = Array(),
      proxyUser = None
    )

    sparkConf.set("spark.executor.instances", "0")

    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.submitArmadaJob(null, clientArguments, armadaJobConfig, sparkConf)
    }
    exception.getMessage should include("Executor count must be greater than 0")
  }

  test("JobTemplateLoader should handle malformed YAML gracefully") {
    val malformedYaml = "invalid: yaml: content: ]]]["
    val templateFile  = tempDir.resolve("malformed-template.yaml").toFile
    val writer        = new FileWriter(templateFile)
    try {
      writer.write(malformedYaml)
    } finally {
      writer.close()
    }

    val exception = intercept[RuntimeException] {
      JobTemplateLoader.loadJobTemplate(templateFile.getAbsolutePath)
    }
    exception.getMessage should include("Failed to parse template as YAML")
    exception.getMessage should include("malformed-template.yaml")
  }

  test("container image should follow precedence: CLI > template > error") {
    val templateWithImage = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = Some(
        PodSpec()
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("template-container")
                .withImage("template-image:1.0")
            )
          )
      )
    )

    val cliConfig = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(1.0),
      containerImage = Some("cli-image:2.0"),
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    val resolvedConfig = armadaClientApp.resolveJobConfig(
      cliConfig,
      Some(templateWithImage),
      Map.empty,
      Map.empty,
      sparkConf
    )

    resolvedConfig.containerImage shouldBe "cli-image:2.0"

    val cliConfigWithoutImage = cliConfig.copy(containerImage = None)
    val resolvedConfigWithTemplateImage = armadaClientApp.resolveJobConfig(
      cliConfigWithoutImage,
      Some(templateWithImage),
      Map.empty,
      Map.empty,
      sparkConf
    )

    resolvedConfigWithTemplateImage.containerImage shouldBe "template-image:1.0"
  }

  test(
    "validateRequiredConfig should require both templates to have container image when CLI image not provided"
  ) {
    val driverTemplateWithImage = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = Some(
        PodSpec()
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("driver")
                .withImage("driver-template-image:1.0")
            )
          )
      )
    )

    val executorTemplateWithImage = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = Some(
        PodSpec()
          .withContainers(
            Seq(
              k8s.io.api.core.v1.generated
                .Container()
                .withName("executor")
                .withImage("executor-template-image:1.0")
            )
          )
      )
    )

    val cliConfigWithoutImage = armadaClientApp.CLIConfig(
      queue = Some("test-queue"),
      jobSetId = Some("test-job-set"),
      namespace = Some("test-namespace"),
      priority = Some(1.0),
      containerImage = None,
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = Some("armada://localhost:50051"),
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = Some(300.seconds),
      driverResources = armadaClientApp.ResourceConfig(None, None, None, None),
      executorResources = armadaClientApp.ResourceConfig(None, None, None, None)
    )

    // Should succeed when both templates have images
    armadaClientApp.validateRequiredConfig(
      cliConfigWithoutImage,
      None,
      Some(driverTemplateWithImage),
      Some(executorTemplateWithImage),
      sparkConf
    )

    // Should fail when only driver template has image
    val exception1 = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(
        cliConfigWithoutImage,
        None,
        Some(driverTemplateWithImage),
        None,
        sparkConf
      )
    }
    exception1.getMessage should include("BOTH driver and executor")

    // Should fail when only executor template has image
    val exception2 = intercept[IllegalArgumentException] {
      armadaClientApp.validateRequiredConfig(
        cliConfigWithoutImage,
        None,
        None,
        Some(executorTemplateWithImage),
        sparkConf
      )
    }
    exception2.getMessage should include("BOTH driver and executor")
  }

  test("missing CLI values should follow precedence rules with templates") {
    val templateFile = createJobTemplateFile("template-queue", "template-job-set")
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)

    // Missing queue from CLI - should use template value
    sparkConf.remove(Config.ARMADA_JOB_QUEUE.key)
    sparkConf.set(Config.ARMADA_JOB_SET_ID.key, "cli-job-set")
    val configWithMissingQueue = armadaClientApp.validateArmadaJobConfig(sparkConf)
    configWithMissingQueue.queue shouldBe "template-queue"
    configWithMissingQueue.jobSetId shouldBe "cli-job-set"

    // Missing jobSetId from CLI - should use template value
    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "cli-queue")
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    val configWithMissingJobSetId = armadaClientApp.validateArmadaJobConfig(sparkConf)
    configWithMissingJobSetId.queue shouldBe "cli-queue"
    configWithMissingJobSetId.jobSetId shouldBe "template-job-set"
  }

}
