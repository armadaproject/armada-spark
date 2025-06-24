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
import k8s.io.api.core.v1.generated.PodSpec
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
}
