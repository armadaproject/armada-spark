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

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path}

class JobTemplateLoaderSuite extends AnyFunSuite with BeforeAndAfter with Matchers {

  private var tempDir: Path = _

  before {
    tempDir = Files.createTempDirectory("job-template-loader-test-")
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
        |  containers:
        |    - name: spark-executor
        |      image: spark:3.5.0
        |      resources:
        |        requests:
        |          cpu: "500m"
        |          memory: "1Gi"
        |        limits:
        |          cpu: "2"
        |          memory: "4Gi"
        |          nvidia.com/gpu: "1"
        |      env:
        |        - name: SECRET_ENV
        |          valueFrom:
        |            secretKeyRef:
        |              name: my-secret
        |              key: password
        |        - name: CONFIG_VALUE
        |          valueFrom:
        |            configMapKeyRef:
        |              name: my-config
        |              key: database-url
        |""".stripMargin

    val templateFile = createTemplateFile("podspec-template.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobItemTemplate(templateFile.getAbsolutePath)

    result.priority shouldBe 1.0
    result.namespace shouldBe "default"
    result.labels should contain("app" -> "spark-executor")
    result.podSpec should not be empty
    val podSpec = result.podSpec.get
    podSpec.restartPolicy shouldBe Some("Never")
    podSpec.terminationGracePeriodSeconds shouldBe Some(30)
    podSpec.nodeSelector should contain("kubernetes.io/hostname" -> "worker-node-1")

    val container = podSpec.containers.head
    val resources = container.resources.get
    resources.requests("cpu").getString shouldBe "500m"
    resources.requests("memory").getString shouldBe "1Gi"
    resources.limits("cpu").getString shouldBe "2"
    resources.limits("memory").getString shouldBe "4Gi"
    resources.limits("nvidia.com/gpu").getString shouldBe "1"

    container.env should have size 2
    val secretEnv = container.env.find(_.name.contains("SECRET_ENV")).get
    secretEnv.valueFrom.get.secretKeyRef.get.getLocalObjectReference.getName shouldBe "my-secret"
    secretEnv.valueFrom.get.secretKeyRef.get.getKey shouldBe "password"

    val configEnv = container.env.find(_.name.contains("CONFIG_VALUE")).get
    configEnv.valueFrom.get.configMapKeyRef.get.localObjectReference.flatMap(_.name) shouldBe Some(
      "my-config"
    )
    configEnv.valueFrom.get.configMapKeyRef.get.key shouldBe Some("database-url")
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

  test("should support all Kubernetes volume types through fabric8 parsing") {
    val yamlContent =
      """priority: 1.0
        |namespace: default
        |podSpec:
        |  volumes:
        |    - name: config-volume
        |      configMap:
        |        name: my-config
        |        defaultMode: 420
        |        optional: false
        |    - name: host-volume
        |      hostPath:
        |        path: /data
        |        type: Directory
        |    - name: csi-volume
        |      csi:
        |        driver: csi.example.com
        |        fsType: ext4
        |        readOnly: false
        |        volumeAttributes:
        |          key1: value1
        |          key2: value2
        |    - name: empty-dir
        |      emptyDir:
        |        medium: Memory
        |        sizeLimit: 1Gi
        |""".stripMargin

    val templateFile = createTemplateFile("volumes-test.yaml", yamlContent)
    val result       = JobTemplateLoader.loadJobItemTemplate(templateFile.getAbsolutePath)

    result.podSpec should not be empty
    val podSpec = result.podSpec.get

    podSpec.volumes should have size 4

    val configVolume = podSpec.volumes.find(_.name.contains("config-volume")).get
    val configMap    = configVolume.volumeSource.get.configMap.get
    configMap.localObjectReference.flatMap(_.name) shouldBe Some("my-config")
    configMap.defaultMode shouldBe Some(420)
    configMap.optional shouldBe Some(false)

    val hostVolume = podSpec.volumes.find(_.name.contains("host-volume")).get
    val hostPath   = hostVolume.volumeSource.get.hostPath.get
    hostPath.path shouldBe Some("/data")
    hostPath.`type` shouldBe Some("Directory")

    val csiVolume = podSpec.volumes.find(_.name.contains("csi-volume")).get
    val csi       = csiVolume.volumeSource.get.csi.get
    csi.driver shouldBe Some("csi.example.com")
    csi.fsType shouldBe Some("ext4")
    csi.readOnly shouldBe Some(false)
    csi.volumeAttributes should contain("key1" -> "value1")
    csi.volumeAttributes should contain("key2" -> "value2")

    val emptyDirVolume = podSpec.volumes.find(_.name.contains("empty-dir")).get
    val emptyDir       = emptyDirVolume.volumeSource.get.emptyDir.get
    emptyDir.medium shouldBe Some("Memory")
    emptyDir.sizeLimit should not be empty
  }
}
