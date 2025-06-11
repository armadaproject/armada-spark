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

import api.submit.{JobSubmitRequest, JobSubmitRequestItem}
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
  private val armadaClientApp = new ArmadaClientApplication()
  private var tempDir: Path = _

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
    // Clean up temp directory and files
    if (tempDir != null) {
      tempDir.toFile.listFiles().foreach(_.delete())
      Files.deleteIfExists(tempDir)
    }
  }

  private def createJobTemplateFile(queue: String, jobSetId: String): File = {
    val templateContent =
      s"""{
         |  "queue": "$queue",
         |  "job_set_id": "$jobSetId",
         |  "job_request_items": [
         |    {
         |      "priority": 1.5,
         |      "namespace": "template-namespace",
         |      "labels": {
         |        "template-label": "template-value"
         |      },
         |      "annotations": {
         |        "template-annotation": "template-value"
         |      }
         |    }
         |  ]
         |}""".stripMargin

    val file = tempDir.resolve("job-template.json").toFile
    val writer = new FileWriter(file)
    try {
      writer.write(templateContent)
    } finally {
      writer.close()
    }
    file
  }

  private def createJobItemTemplateFile(priority: Double, namespace: String, filename: String = "job-item-template.json"): File = {
    val templateContent =
      s"""{
         |  "priority": $priority,
         |  "namespace": "$namespace",
         |  "labels": {
         |    "item-template-label": "item-template-value"
         |  },
         |  "annotations": {
         |    "item-template-annotation": "item-template-value"
         |  }
         |}""".stripMargin

    val file = tempDir.resolve(filename).toFile
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

    config.queue shouldBe "test-queue"
    config.jobSetId shouldBe "test-job-set"
    config.namespace shouldBe "test-namespace"
    config.priority shouldBe 1.0
    config.containerImage shouldBe "spark:3.5.0"
    config.jobTemplate shouldBe None
    config.driverJobItemTemplate shouldBe None
    config.executorJobItemTemplate shouldBe None
  }

  test("validateArmadaJobConfig should use queue from template when CLI config not provided") {
    sparkConf.remove(Config.ARMADA_JOB_QUEUE.key)
    
    // Create a real template file with queue
    val templateFile = createJobTemplateFile("template-queue", "template-job-set")
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    
    // Should use queue from template since CLI config is not provided
    config.queue shouldBe "template-queue"
    config.jobSetId shouldBe "test-job-set"  // CLI config takes precedence
    config.jobTemplate should not be empty
    config.jobTemplate.get.queue shouldBe "template-queue"
    config.jobTemplate.get.jobSetId shouldBe "template-job-set"
  }

  test("validateArmadaJobConfig should use jobSetId from template when CLI config not provided") {
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    
    // Create a real template file with jobSetId
    val templateFile = createJobTemplateFile("test-queue", "template-job-set")
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    
    // Should use jobSetId from template since CLI config is not provided
    config.queue shouldBe "test-queue"  // CLI config takes precedence
    config.jobSetId shouldBe "template-job-set"  // Template value used
    config.jobTemplate should not be empty
    config.jobTemplate.get.jobSetId shouldBe "template-job-set"
  }

  test("validateArmadaJobConfig should use app ID fallback when no template and no CLI jobSetId") {
    sparkConf.remove(Config.ARMADA_JOB_SET_ID.key)
    sparkConf.set("spark.app.id", "test-app-id")
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    
    // Should fallback to app ID when no template and no CLI config
    config.jobSetId shouldBe "test-app-id"
  }

  test("validateArmadaJobConfig should reject empty queue") {
    sparkConf.set(Config.ARMADA_JOB_QUEUE.key, "")
    
    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    exception.getMessage should include("Queue name must be set")
  }

  test("validateArmadaJobConfig should reject empty jobSetId") {
    sparkConf.set(Config.ARMADA_JOB_SET_ID.key, "")
    
    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    exception.getMessage should include("Empty jobSetId is not allowed")
  }

  test("validateArmadaJobConfig should require container image") {
    sparkConf.remove(Config.CONTAINER_IMAGE.key)
    
    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    exception.getMessage should include("Container image must be set")
  }

  test("validateArmadaJobConfig should reject empty container image") {
    sparkConf.set(Config.CONTAINER_IMAGE.key, "")
    
    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    exception.getMessage should include("Empty container image is not allowed")
  }

  test("validateArmadaJobConfig should require either nodeSelectors or gangUniformityLabel") {
    sparkConf.remove(Config.ARMADA_JOB_NODE_SELECTORS.key)
    
    val exception = intercept[IllegalArgumentException] {
      armadaClientApp.validateArmadaJobConfig(sparkConf)
    }
    exception.getMessage should include("Either")
    exception.getMessage should include("must be set")
  }

  test("validateArmadaJobConfig should accept gangUniformityLabel instead of nodeSelectors") {
    sparkConf.remove(Config.ARMADA_JOB_NODE_SELECTORS.key)
    sparkConf.set(Config.ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY.key, "zone")
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    
    config.nodeSelectors shouldBe Map.empty
    config.nodeUniformityLabel shouldBe Some("zone")
  }

  test("validateArmadaJobConfig should load driver job item template") {
    val driverTemplateFile = createJobItemTemplateFile(2.5, "driver-namespace")
    sparkConf.set(Config.ARMADA_DRIVER_JOB_ITEM_TEMPLATE.key, driverTemplateFile.getAbsolutePath)
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    
    config.driverJobItemTemplate should not be empty
    config.driverJobItemTemplate.get.priority shouldBe 2.5
    config.driverJobItemTemplate.get.namespace shouldBe "driver-namespace"
    config.driverJobItemTemplate.get.labels should contain("item-template-label" -> "item-template-value")
  }

  test("validateArmadaJobConfig should load executor job item template") {
    val executorTemplateFile = createJobItemTemplateFile(3.0, "executor-namespace")
    sparkConf.set(Config.ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE.key, executorTemplateFile.getAbsolutePath)
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    
    config.executorJobItemTemplate should not be empty
    config.executorJobItemTemplate.get.priority shouldBe 3.0
    config.executorJobItemTemplate.get.namespace shouldBe "executor-namespace"
    config.executorJobItemTemplate.get.labels should contain("item-template-label" -> "item-template-value")
  }

  test("validateArmadaJobConfig should load all three template types simultaneously") {
    val jobTemplateFile = createJobTemplateFile("all-template-queue", "all-template-job-set")
    val driverTemplateFile = createJobItemTemplateFile(1.5, "all-driver-namespace", "driver-template.json")
    val executorTemplateFile = createJobItemTemplateFile(2.5, "all-executor-namespace", "executor-template.json")
    
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, jobTemplateFile.getAbsolutePath)
    sparkConf.set(Config.ARMADA_DRIVER_JOB_ITEM_TEMPLATE.key, driverTemplateFile.getAbsolutePath)
    sparkConf.set(Config.ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE.key, executorTemplateFile.getAbsolutePath)
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    
    // Job template
    config.jobTemplate should not be empty
    config.jobTemplate.get.queue shouldBe "all-template-queue"
    
    // Driver template
    config.driverJobItemTemplate should not be empty
    config.driverJobItemTemplate.get.namespace shouldBe "all-driver-namespace"
    
    // Executor template
    config.executorJobItemTemplate should not be empty
    config.executorJobItemTemplate.get.namespace shouldBe "all-executor-namespace"
    
    // CLI config should override template values
    config.queue shouldBe "test-queue"  // CLI value, not template value
    config.jobSetId shouldBe "test-job-set"  // CLI value, not template value
  }

  // Tests for mergeTemplateWithConfig method using real template files
  test("mergeTemplateWithConfig should use config values over template values") {
    // Create a real template file
    val templateFile = createJobTemplateFile("template-queue", "template-job-set")
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)
    
    // Load the template through validateArmadaJobConfig to get the complete config
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    val template = config.jobTemplate.get
    
    // Use reflection to access the private method
    val method = armadaClientApp.getClass.getDeclaredMethod(
      "mergeTemplateWithConfig",
      classOf[JobSubmitRequest],
      classOf[armadaClientApp.ArmadaJobConfig],
      classOf[SparkConf]
    )
    method.setAccessible(true)
    
    val result = method.invoke(
      armadaClientApp,
      template,
      config,
      sparkConf
    ).asInstanceOf[(String, String, Map[String, String], Map[String, String])]
    
    // Config values should override template values
    result._1 shouldBe "test-queue"  // queue from CLI config
    result._2 shouldBe "test-job-set"  // jobSetId from CLI config  
    result._3 shouldBe Map("template-annotation" -> "template-value")  // template annotations
    result._4 shouldBe Map("template-label" -> "template-value")  // template labels
  }

  test("mergeTemplateWithConfig should handle empty job request items") {
    // Create a template file with empty job request items
    val templateContent =
      """{
        |  "queue": "template-queue",
        |  "job_set_id": "template-job-set",
        |  "job_request_items": []
        |}""".stripMargin

    val templateFile = tempDir.resolve("empty-items-template.json").toFile
    val writer = new FileWriter(templateFile)
    try {
      writer.write(templateContent)
    } finally {
      writer.close()
    }
    
    sparkConf.set(Config.ARMADA_JOB_TEMPLATE.key, templateFile.getAbsolutePath)
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    val template = config.jobTemplate.get
    
    val method = armadaClientApp.getClass.getDeclaredMethod(
      "mergeTemplateWithConfig",
      classOf[JobSubmitRequest],
      classOf[armadaClientApp.ArmadaJobConfig],
      classOf[SparkConf]
    )
    method.setAccessible(true)
    
    val result = method.invoke(
      armadaClientApp,
      template,
      config,
      sparkConf
    ).asInstanceOf[(String, String, Map[String, String], Map[String, String])]
    
    result._1 shouldBe "test-queue"  // CLI config
    result._2 shouldBe "test-job-set"  // CLI config
    result._3 shouldBe Map.empty  // empty annotations
    result._4 shouldBe Map.empty  // empty labels
  }

  // Tests for mergeDriverTemplate method using real template files
  test("mergeDriverTemplate should merge template with runtime configuration") {
    val driverTemplateFile = createJobItemTemplateFile(0.5, "template-namespace")
    sparkConf.set(Config.ARMADA_DRIVER_JOB_ITEM_TEMPLATE.key, driverTemplateFile.getAbsolutePath)
    
    val config = armadaClientApp.validateArmadaJobConfig(sparkConf)
    val template = config.driverJobItemTemplate.get
    
    val runtimeAnnotations = Map("runtime-annotation" -> "runtime-value")
    val runtimeLabels = Map("runtime-label" -> "runtime-value")
    
    val method = armadaClientApp.getClass.getDeclaredMethod(
      "mergeDriverTemplate",
      classOf[JobSubmitRequestItem],
      classOf[armadaClientApp.ArmadaJobConfig],
      classOf[Map[String, String]],
      classOf[Map[String, String]],
      classOf[Int],
      classOf[String],
      classOf[Seq[Volume]],
      classOf[Seq[VolumeMount]],
      classOf[Map[String, String]],
      classOf[Seq[String]],
      classOf[SparkConf]
    )
    method.setAccessible(true)
    
    val result = method.invoke(
      armadaClientApp,
      template,
      config,
      runtimeAnnotations,
      runtimeLabels,
      7078: Integer,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Map("node-type" -> "compute"),
      Seq("--arg1", "--arg2"),
      sparkConf
    ).asInstanceOf[JobSubmitRequestItem]
    
    // Runtime config should override template values
    result.priority shouldBe 1.0  // runtime priority from sparkConf
    result.namespace shouldBe "test-namespace"  // runtime namespace from sparkConf
    
    // Annotations and labels should be merged (runtime takes precedence)
    result.annotations should contain("item-template-annotation" -> "item-template-value")
    result.annotations should contain("runtime-annotation" -> "runtime-value")
    result.labels should contain("item-template-label" -> "item-template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
    
    // Hardcoded values should always be present
    result.podSpec should not be empty
    result.services should not be empty
  }

  // Tests for mergeExecutorTemplate method
  test("mergeExecutorTemplate should merge template with runtime configuration") {
    val template = JobSubmitRequestItem(
      priority = 0.5,
      namespace = "template-namespace",
      annotations = Map("template-annotation" -> "template-value"),
      labels = Map("template-label" -> "template-value"),
      podSpec = Some(PodSpec())
    )
    
    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      namespace = "runtime-namespace",
      priority = 3.0,
      containerImage = "spark:3.5.0",
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = "armada://localhost:50051",
      nodeSelectors = Map("node-type" -> "compute"),
      nodeUniformityLabel = Some("zone"),
      executorConnectionTimeout = 300.seconds,
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = Some(template)
    )
    
    val runtimeAnnotations = Map("runtime-annotation" -> "runtime-value")
    val runtimeLabels = Map("runtime-label" -> "runtime-value")
    val javaOptEnvVars = Seq(EnvVar().withName("SPARK_JAVA_OPT_0").withValue("-Xmx1g"))
    
    val method = armadaClientApp.getClass.getDeclaredMethod(
      "mergeExecutorTemplate",
      classOf[JobSubmitRequestItem],
      classOf[Int],
      classOf[armadaClientApp.ArmadaJobConfig],
      classOf[Map[String, String]],
      classOf[Map[String, String]],
      classOf[Seq[EnvVar]],
      classOf[String],
      classOf[Int],
      classOf[Seq[Volume]],
      classOf[Map[String, String]],
      classOf[SparkConf]
    )
    method.setAccessible(true)
    
    val result = method.invoke(
      armadaClientApp,
      template,
      0: Integer,
      armadaJobConfig,
      runtimeAnnotations,
      runtimeLabels,
      javaOptEnvVars,
      "driver-service",
      7078: Integer,
      Seq.empty[Volume],
      Map("node-type" -> "compute"),
      sparkConf
    ).asInstanceOf[JobSubmitRequestItem]
    
    // Runtime config should override template values
    result.priority shouldBe 3.0  // runtime priority
    result.namespace shouldBe "runtime-namespace"  // runtime namespace
    
    // Annotations and labels should be merged (runtime takes precedence)
    result.annotations should contain("template-annotation" -> "template-value")
    result.annotations should contain("runtime-annotation" -> "runtime-value")
    result.labels should contain("template-label" -> "template-value")
    result.labels should contain("runtime-label" -> "runtime-value")
    
    // Hardcoded values should always be present
    result.podSpec should not be empty
    result.podSpec.get.initContainers should not be empty
    result.podSpec.get.containers should not be empty
  }

  test("mergeDriverTemplate should handle template without podSpec") {
    val template = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = None
    )
    
    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set", 
      namespace = "runtime-namespace",
      priority = 2.0,
      containerImage = "spark:3.5.0",
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = "armada://localhost:50051",
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = 300.seconds,
      jobTemplate = None,
      driverJobItemTemplate = Some(template),
      executorJobItemTemplate = None
    )
    
    val method = armadaClientApp.getClass.getDeclaredMethod(
      "mergeDriverTemplate",
      classOf[JobSubmitRequestItem],
      classOf[armadaClientApp.ArmadaJobConfig],
      classOf[Map[String, String]],
      classOf[Map[String, String]],
      classOf[Int],
      classOf[String],
      classOf[Seq[Volume]],
      classOf[Seq[VolumeMount]],
      classOf[Map[String, String]],
      classOf[Seq[String]],
      classOf[SparkConf]
    )
    method.setAccessible(true)
    
    val result = method.invoke(
      armadaClientApp,
      template,
      armadaJobConfig,
      Map.empty[String, String],
      Map.empty[String, String],
      7078: Integer,
      "org.example.TestClass",
      Seq.empty[Volume],
      Seq.empty[VolumeMount],
      Map.empty[String, String],
      Seq.empty[String],
      sparkConf
    ).asInstanceOf[JobSubmitRequestItem]
    
    // Should create a default PodSpec when template doesn't have one
    result.podSpec should not be empty
    result.podSpec.get.restartPolicy shouldBe Some("Never")
    result.podSpec.get.terminationGracePeriodSeconds shouldBe Some(0)
  }

  test("mergeExecutorTemplate should handle template without podSpec") {
    val template = JobSubmitRequestItem(
      priority = 1.0,
      namespace = "template-namespace",
      annotations = Map.empty,
      labels = Map.empty,
      podSpec = None
    )
    
    val armadaJobConfig = armadaClientApp.ArmadaJobConfig(
      queue = "test-queue",
      jobSetId = "test-job-set",
      namespace = "runtime-namespace",
      priority = 2.0,
      containerImage = "spark:3.5.0",
      podLabels = Map.empty,
      driverLabels = Map.empty,
      executorLabels = Map.empty,
      armadaClusterUrl = "armada://localhost:50051",
      nodeSelectors = Map.empty,
      nodeUniformityLabel = None,
      executorConnectionTimeout = 300.seconds,
      jobTemplate = None,
      driverJobItemTemplate = None,
      executorJobItemTemplate = Some(template)
    )
    
    val method = armadaClientApp.getClass.getDeclaredMethod(
      "mergeExecutorTemplate",
      classOf[JobSubmitRequestItem],
      classOf[Int],
      classOf[armadaClientApp.ArmadaJobConfig],
      classOf[Map[String, String]],
      classOf[Map[String, String]],
      classOf[Seq[EnvVar]],
      classOf[String],
      classOf[Int],
      classOf[Seq[Volume]],
      classOf[Map[String, String]],
      classOf[SparkConf]
    )
    method.setAccessible(true)
    
    val result = method.invoke(
      armadaClientApp,
      template,
      1: Integer,
      armadaJobConfig,
      Map.empty[String, String],
      Map.empty[String, String],
      Seq.empty[EnvVar],
      "driver-service",
      7078: Integer,
      Seq.empty[Volume],
      Map.empty[String, String],
      sparkConf
    ).asInstanceOf[JobSubmitRequestItem]
    
    // Should create a default PodSpec when template doesn't have one
    result.podSpec should not be empty
    result.podSpec.get.restartPolicy shouldBe Some("Never")
    result.podSpec.get.terminationGracePeriodSeconds shouldBe Some(0)
    result.podSpec.get.initContainers should not be empty
    result.podSpec.get.containers should not be empty
  }
}