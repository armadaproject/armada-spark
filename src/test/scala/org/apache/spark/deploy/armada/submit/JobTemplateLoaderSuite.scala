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

import io.fabric8.kubernetes.client.DefaultKubernetesClient
import io.fabric8.kubernetes.client.utils.Serialization
import k8s.io.api.core.v1.generated.{Container, PodSpec}
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.armada.Config
import org.apache.spark.deploy.k8s.submit.{KubernetesDriverBuilder, PythonMainAppResource => KPMainAppResource}
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesExecutorConf}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBuilder
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path}
class JobTemplateLoaderSuite extends AnyFunSuite with BeforeAndAfter with Matchers {
  private var sparkConf: SparkConf = _

  private var tempDir: Path = _

  before {
    tempDir = Files.createTempDirectory("job-template-loader-test-")

    sparkConf = new SparkConf()
      .set("spark.master", "armada://localhost:50051")
      .set("spark.app.name", "test-app")
      .set(Config.ARMADA_JOB_QUEUE.key, "test-queue")
      .set(Config.ARMADA_JOB_SET_ID.key, "test-job-set")
      .set(Config.ARMADA_SPARK_JOB_NAMESPACE.key, "test-namespace")
      .set(Config.ARMADA_SPARK_JOB_PRIORITY.key, 100.toString)
      .set(Config.CONTAINER_IMAGE.key, "spark:3.5.0")
      .set(Config.ARMADA_JOB_NODE_SELECTORS.key, "node-type=compute")
      .set("spark.kubernetes.container.image", "hig1")
      .set("spark.kubernetes.file.upload.path", "/tmp")

  }

  after {
    // Clean up temp directory and files
    if (tempDir != null) {
      tempDir.toFile.listFiles().foreach(_.delete())
      Files.deleteIfExists(tempDir)
    }
  }

  private def createTemplateFile(filename: String, content: String): File = {
    val file   = tempDir.resolve(filename).toFile
    val writer = new FileWriter(file)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
    file
  }

  test("should parse valid JobSubmitRequest YAML") {
    val yamlContent =
      """queue: test-queue
        |jobSetId: test-job-set
        |jobRequestItems: []
        |""".stripMargin

    val templateFile = createTemplateFile("job-template.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobTemplate(templateFile.getAbsolutePath)

    result.queue shouldBe "test-queue"
    result.jobSetId shouldBe "test-job-set"
    result.jobRequestItems shouldBe empty
  }

  test("should parse JobSubmitRequest with complex jobRequestItems") {
    val yamlContent =
      """queue: production-queue
        |jobSetId: spark-pi-job-set
        |jobRequestItems:
        |  - priority: 2.0
        |    namespace: production
        |    labels:
        |      app: spark-pi
        |      component: driver
        |    annotations:
        |      scheduler: armada
        |      version: "1.0"
        |  - priority: 1.0
        |    namespace: production
        |    labels:
        |      app: spark-pi
        |      component: executor
        |""".stripMargin

    val templateFile = createTemplateFile("complex-job-template.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobTemplate(templateFile.getAbsolutePath)

    result.queue shouldBe "production-queue"
    result.jobSetId shouldBe "spark-pi-job-set"
    result.jobRequestItems should have size 2

    val driverItem = result.jobRequestItems.head
    driverItem.priority shouldBe 2.0
    driverItem.namespace shouldBe "production"
    driverItem.labels should contain("app" -> "spark-pi")
    driverItem.labels should contain("component" -> "driver")
    driverItem.annotations should contain("scheduler" -> "armada")
  }

  test("should handle missing fields with empty values") {
    val yamlContent =
      """queue: test-queue
        |jobRequestItems: []
        |""".stripMargin

    val templateFile = createTemplateFile("incomplete-template.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobTemplate(templateFile.getAbsolutePath)

    result.queue shouldBe "test-queue"
    result.jobSetId shouldBe ""
    result.jobRequestItems shouldBe empty
  }

  test("should parse valid JobSubmitRequestItem YAML") {
    val yamlContent =
      """priority: 2.5
        |namespace: production
        |labels:
        |  app: spark
        |  component: driver
        |annotations:
        |  scheduler: armada
        |  version: "1.0"
        |""".stripMargin

    val templateFile = createTemplateFile("job-item-template.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobItemTemplate(templateFile.getAbsolutePath)

    result.priority shouldBe 2.5
    result.namespace shouldBe "production"
    result.labels should contain("app" -> "spark")
    result.labels should contain("component" -> "driver")
    result.annotations should contain("scheduler" -> "armada")
    result.annotations should contain("version" -> "1.0")
  }

  test("should handle minimal JobSubmitRequestItem") {
    val yamlContent =
      """priority: 0.0
        |namespace: default
        |""".stripMargin

    val templateFile = createTemplateFile("minimal-template.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobItemTemplate(templateFile.getAbsolutePath)

    result.priority shouldBe 0.0
    result.namespace shouldBe "default"
    result.labels shouldBe empty
    result.annotations shouldBe empty
    result.podSpec shouldBe None
  }

  test("should parse JobSubmitRequestItem with complex podSpec") {
    val yamlContent =
      """priority: 1.0
        |namespace: default
        |labels:
        |  app: spark-executor
        |podSpec:
        |  restartPolicy: Never
        |  terminationGracePeriodSeconds: 30
        |  nodeSelector:
        |    kubernetes.io/hostname: worker-node-1
        |  tolerations:
        |    - key: "dedicated"
        |      operator: "Equal"
        |      value: "spark"
        |      effect: "NoSchedule"
        |""".stripMargin

    val templateFile = createTemplateFile("podspec-template.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobItemTemplate(templateFile.getAbsolutePath)

    result.priority shouldBe 1.0
    result.namespace shouldBe "default"
    result.labels should contain("app" -> "spark-executor")
    result.podSpec should not be empty
    result.podSpec.get.restartPolicy shouldBe Some("Never")
    result.podSpec.get.terminationGracePeriodSeconds shouldBe Some(30)
    result.podSpec.get.nodeSelector should contain("kubernetes.io/hostname" -> "worker-node-1")
  }

  test("should fail with clear errors for invalid inputs") {
    val nonExistentException = intercept[RuntimeException] {
      JobTemplateLoader.loadJobTemplate("/non/existent/path.yaml")
    }
    nonExistentException.getMessage should include("Failed to load template from file")

    val invalidYaml = """queue: test\njobSetId: [unclosed array"""
    val invalidFile = createTemplateFile("invalid.yaml", invalidYaml)
    val invalidYamlException = intercept[RuntimeException] {
      JobTemplateLoader.loadJobTemplate(invalidFile.getAbsolutePath)
    }
    invalidYamlException.getMessage should include("Failed to parse template")

    val malformedYaml =
      """priority: "not-a-number"
        |namespace: 123
        |invalid: [unclosed array
        |""".stripMargin
    val malformedFile = createTemplateFile("malformed.yaml", malformedYaml)
    val malformedException = intercept[RuntimeException] {
      JobTemplateLoader.loadJobItemTemplate(malformedFile.getAbsolutePath)
    }
    malformedException.getMessage should include("Failed to parse template")
  }

  test("should ignore unknown Kubernetes fields and handle various formats") {
    val yamlWithK8sFields =
      """apiVersion: armada.io/v1
        |kind: JobTemplate
        |metadata:
        |  name: my-job-template
        |  namespace: default
        |queue: k8s-compatible-queue
        |jobSetId: k8s-compatible-job-set
        |jobRequestItems: []
        |""".stripMargin

    val templateFile = createTemplateFile("k8s-template.conf", yamlWithK8sFields)
    val result       = JobTemplateLoader.loadJobTemplate(templateFile.getAbsolutePath)

    result.queue shouldBe "k8s-compatible-queue"
    result.jobSetId shouldBe "k8s-compatible-job-set"
    result.jobRequestItems shouldBe empty
  }

  test("should handle file URIs and different path formats") {
    val yamlContent =
      """priority: 4.0
        |namespace: file-uri-namespace
        |labels:
        |  source: file-uri-test
        |""".stripMargin

    val templateFile = createTemplateFile("file-uri-template.yaml", yamlContent)

    val fileUri = s"file://${templateFile.getAbsolutePath}"
    val result  = JobTemplateLoader.loadJobItemTemplate(fileUri)

    result.priority shouldBe 4.0
    result.namespace shouldBe "file-uri-namespace"
    result.labels should contain("source" -> "file-uri-test")
  }

  test("driverSpec") {
    val driverSpec = new KubernetesDriverBuilder().buildFromFeatures(
      new KubernetesDriverConf(
        sparkConf = sparkConf.clone(),
        appId = "",
        mainAppResource = KPMainAppResource("/tmp/test/pi.py"),
        mainClass = "org.apache.spark.deploy.PythonRunner",
        appArgs = Array("100"),
        proxyUser = None
      ),
      new DefaultKubernetesClient()
    )
    println("gbjD: " + driverSpec)
    driverSpec.pod.pod.getSpec.setVolumes(null)
    driverSpec.pod.pod.setApiVersion(null)
    val yamlString = Serialization.asYaml(driverSpec.pod.pod.getSpec)

    println("gbjyamlDriver: " + yamlString)
    val result: PodSpec = JobTemplateLoader.unmarshal(yamlString, classOf[PodSpec], "driver")
    driverSpec.pod.container.setResources(null)
    val containerString = Serialization.asYaml(driverSpec.pod.container)
    val containerResult: Container =
      JobTemplateLoader.unmarshal(containerString, classOf[Container], "driver")
    containerResult.getImage shouldBe "hig1"

    val executorConf = new KubernetesExecutorConf(
      sparkConf = sparkConf.clone(),
      appId = "appId",
      executorId = "execId",
      driverPod = None,
      resourceProfileId = 1
    )
    val executorSpec = new KubernetesExecutorBuilder().buildFromFeatures(
      executorConf,
      new SecurityManager(sparkConf),
      new DefaultKubernetesClient(),
      new ResourceProfile(executorResources = null, taskResources = null)
    )
    executorSpec.pod.pod.getSpec.setVolumes(null)
    val execPodString = Serialization.asYaml(executorSpec.pod.pod.getSpec)
    val execPod: PodSpec =
      JobTemplateLoader.unmarshal(execPodString, classOf[PodSpec], "executor pod")
    executorSpec.pod.container.setResources(null)
    val execContainerString = Serialization.asYaml(executorSpec.pod.container)
    val execContainer: Container =
      JobTemplateLoader.unmarshal(execContainerString, classOf[Container], "executor container")
    execContainer.getImage shouldBe "hig1"

  }

}
