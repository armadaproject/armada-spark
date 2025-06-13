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

import org.apache.spark.deploy.armada.submit.JobTemplateLoaderExceptions._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import api.submit.JobSubmitRequest

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path}

class JobTemplateLoaderSuite extends AnyFunSuite with BeforeAndAfter with Matchers {
  private var tempDir: Path  = _
  private var testFile: File = _
  private val sampleJobTemplate =
    """{
      |  "priority": 1.0,
      |  "namespace": "default",
      |  "labels": {
      |    "app": "spark-job"
      |  },
      |  "annotations": {
      |    "armada.scheduler": "default"
      |  },
      |  "pod_specs": [
      |    {
      |      "containers": [
      |        {
      |          "name": "spark-driver",
      |          "image": "spark:latest",
      |          "resources": {
      |            "requests": {
      |              "cpu": "1",
      |              "memory": "1Gi"
      |            }
      |          }
      |        }
      |      ]
      |    }
      |  ]
      |}""".stripMargin

  before {
    tempDir = Files.createTempDirectory("job-template-test-")
    testFile = tempDir.resolve("test-template.json").toFile

    val writer = new FileWriter(testFile)
    try {
      writer.write(sampleJobTemplate)
    } finally {
      writer.close()
    }
  }

  after {
    if (testFile.exists()) testFile.delete()
    Files.deleteIfExists(tempDir)
  }

  test("loadTemplate should successfully load local file without protocol") {
    val result = JobTemplateLoader.loadJobTemplate(testFile.getAbsolutePath)
    result shouldBe a[JobSubmitRequest]
    result.jobRequestItems should have size 1
    val item = result.jobRequestItems.head
    item.priority shouldBe 1.0
    item.namespace shouldBe "default"
    item.labels should contain("app" -> "spark-job")
  }

  test("loadTemplate should successfully load local file with file:// protocol") {
    val fileUri = testFile.toURI.toString
    val result  = JobTemplateLoader.loadJobTemplate(fileUri)
    result shouldBe a[JobSubmitRequest]
    result.jobRequestItems should have size 1
    val item = result.jobRequestItems.head
    item.priority shouldBe 1.0
    item.namespace shouldBe "default"
    item.labels should contain("app" -> "spark-job")
  }

  test("loadTemplate should throw FileNotFoundException for non-existent file") {
    val nonExistentPath = tempDir.resolve("non-existent.json").toString

    assertThrows[FileNotFoundException] {
      JobTemplateLoader.loadJobTemplate(nonExistentPath)
    }
  }

  test("loadTemplate should throw JobTemplateLoadException for directory path") {
    assertThrows[JobTemplateLoadException] {
      JobTemplateLoader.loadJobTemplate(tempDir.toString)
    }
  }

  test("loadTemplate should throw IllegalArgumentException for empty path") {
    assertThrows[IllegalArgumentException] {
      JobTemplateLoader.loadJobTemplate("")
    }
  }

  test("loadTemplate should throw UnsupportedProtocolException for unsupported protocol") {
    assertThrows[UnsupportedProtocolException] {
      JobTemplateLoader.loadJobTemplate("ftp://example.com/template.json")
    }
  }

  test("loadTemplate should handle file:// protocol with non-existent file") {
    val nonExistentFileUri = "file://" + tempDir.resolve("missing.json").toString

    assertThrows[FileNotFoundException] {
      JobTemplateLoader.loadJobTemplate(nonExistentFileUri)
    }
  }

  test("loadTemplate should handle invalid JSON gracefully") {
    val invalidJsonFile = tempDir.resolve("invalid.json").toFile
    val writer          = new FileWriter(invalidJsonFile)
    try {
      writer.write("{ invalid json content")
    } finally {
      writer.close()
    }

    try {
      assertThrows[JobTemplateLoadException] {
        JobTemplateLoader.loadJobTemplate(invalidJsonFile.getAbsolutePath)
      }
    } finally {
      invalidJsonFile.delete()
    }
  }

  test("loadTemplate should create proper error messages") {
    val exception = intercept[FileNotFoundException] {
      JobTemplateLoader.loadJobTemplate("/non/existent/path/template.json")
    }
    exception.getMessage should include("File not found: /non/existent/path/template.json")
  }

  test("loadTemplate should handle JSON file extension") {
    val jsonFile = tempDir.resolve("test-template-simple.json").toFile
    val writer   = new FileWriter(jsonFile)
    try {
      writer.write("""{"priority": 2.0, "namespace": "test"}""")
    } finally {
      writer.close()
    }

    try {
      val result = JobTemplateLoader.loadJobTemplate(jsonFile.getAbsolutePath)
      result shouldBe a[JobSubmitRequest]
      result.jobRequestItems should have size 1
      val item = result.jobRequestItems.head
      item.priority shouldBe 2.0
      item.namespace shouldBe "test"
    } finally {
      jsonFile.delete()
    }
  }

  test("loadTemplate should parse JSON with special characters") {
    val contentWithSpecialChars =
      """{
        |  "priority": 1.5,
        |  "namespace": "special-ns",
        |  "labels": {
        |    "special": "éñ中文",
        |    "unicode": "🚀✨"
        |  }
        |}""".stripMargin

    val specialFile = tempDir.resolve("special.json").toFile
    val writer      = new FileWriter(specialFile, java.nio.charset.StandardCharsets.UTF_8)
    try {
      writer.write(contentWithSpecialChars)
    } finally {
      writer.close()
    }

    try {
      val result = JobTemplateLoader.loadJobTemplate(specialFile.getAbsolutePath)
      result shouldBe a[JobSubmitRequest]
      result.jobRequestItems should have size 1
      val item = result.jobRequestItems.head
      item.priority shouldBe 1.5
      item.namespace shouldBe "special-ns"
      item.labels should contain("special" -> "éñ中文")
      item.labels should contain("unicode" -> "🚀✨")
    } finally {
      specialFile.delete()
    }
  }

  // Tests for JobSubmitRequestItem templates

  private val sampleJobItemTemplate =
    """{
      |  "priority": 2.0,
      |  "namespace": "test-namespace",
      |  "labels": {
      |    "component": "driver",
      |    "env": "test"
      |  },
      |  "annotations": {
      |    "spark.version": "3.5.0"
      |  }
      |}""".stripMargin

  test("loadJobItemTemplate should successfully load local file") {
    val itemFile = tempDir.resolve("item-template.json").toFile
    val writer   = new FileWriter(itemFile)
    try {
      writer.write(sampleJobItemTemplate)
    } finally {
      writer.close()
    }

    try {
      val result = JobTemplateLoader.loadJobItemTemplate(itemFile.getAbsolutePath)
      result shouldBe a[api.submit.JobSubmitRequestItem]
      result.priority shouldBe 2.0
      result.namespace shouldBe "test-namespace"
      result.labels should contain("component" -> "driver")
      result.labels should contain("env" -> "test")
      result.annotations should contain("spark.version" -> "3.5.0")
    } finally {
      itemFile.delete()
    }
  }

  test("loadJobItemTemplate should successfully load local file with file:// protocol") {
    val itemFile = tempDir.resolve("item-template-file.json").toFile
    val writer   = new FileWriter(itemFile)
    try {
      writer.write(sampleJobItemTemplate)
    } finally {
      writer.close()
    }

    try {
      val fileUri = itemFile.toURI.toString
      val result  = JobTemplateLoader.loadJobItemTemplate(fileUri)
      result shouldBe a[api.submit.JobSubmitRequestItem]
      result.priority shouldBe 2.0
      result.namespace shouldBe "test-namespace"
      result.labels should contain("component" -> "driver")
      result.annotations should contain("spark.version" -> "3.5.0")
    } finally {
      itemFile.delete()
    }
  }

  test("loadJobItemTemplate should throw FileNotFoundException for non-existent file") {
    val nonExistentPath = tempDir.resolve("non-existent-item.json").toString

    assertThrows[FileNotFoundException] {
      JobTemplateLoader.loadJobItemTemplate(nonExistentPath)
    }
  }

  test("loadJobItemTemplate should throw IllegalArgumentException for empty path") {
    assertThrows[IllegalArgumentException] {
      JobTemplateLoader.loadJobItemTemplate("")
    }
  }

  test("loadJobItemTemplate should handle invalid JSON gracefully") {
    val invalidJsonFile = tempDir.resolve("invalid-item.json").toFile
    val writer          = new FileWriter(invalidJsonFile)
    try {
      writer.write("{ invalid json item content")
    } finally {
      writer.close()
    }

    try {
      assertThrows[JobTemplateLoadException] {
        JobTemplateLoader.loadJobItemTemplate(invalidJsonFile.getAbsolutePath)
      }
    } finally {
      invalidJsonFile.delete()
    }
  }

  test("loadJobItemTemplate should parse minimal JSON structure") {
    val minimalTemplate = """{
      |  "priority": 0.5
      |}""".stripMargin

    val minimalFile = tempDir.resolve("minimal-item.json").toFile
    val writer      = new FileWriter(minimalFile)
    try {
      writer.write(minimalTemplate)
    } finally {
      writer.close()
    }

    try {
      val result = JobTemplateLoader.loadJobItemTemplate(minimalFile.getAbsolutePath)
      result shouldBe a[api.submit.JobSubmitRequestItem]
      result.priority shouldBe 0.5
      result.namespace shouldBe "" // Empty string when not specified in JSON
      result.labels shouldBe Map.empty
      result.annotations shouldBe Map.empty
    } finally {
      minimalFile.delete()
    }
  }
}
