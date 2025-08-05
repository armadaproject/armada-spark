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

package org.apache.spark.deploy.armada

import org.scalatest.funsuite.AnyFunSuite
import Config._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers

class ConfigSuite extends AnyFunSuite with TableDrivenPropertyChecks with Matchers {
  test("commaSeparatedLabelsToMap") {
    val testCases = Table(
      // columns
      ("labels", "expected"),
      // rows
      ("", Map.empty[String, String]),
      ("a=1", Map("a" -> "1")),
      ("a=1,b=2", Map("a" -> "1", "b" -> "2")),
      ("a=1,b=2,c=3", Map("a" -> "1", "b" -> "2", "c" -> "3")),
      ("a=1,,b=2,c=3", Map("a" -> "1", "b" -> "2", "c" -> "3")),
      (" a=1,b= 2, c = 3 ", Map("a" -> "1", "b" -> "2", "c" -> "3"))
    )

    forAll(testCases) { (labels, expected) =>
      commaSeparatedLabelsToMap(labels) shouldEqual expected
    }
  }

  test("commaSeparatedAnnotationsToMap") {
    val testCases = Table(
      // columns
      ("annotations", "expected"),
      // rows
      ("", Map.empty[String, String]),
      (
        "nginx.ingress.kubernetes.io/rewrite-target=/",
        Map("nginx.ingress.kubernetes.io/rewrite-target" -> "/")
      ),
      ("key1=value1,key2=value2", Map("key1" -> "value1", "key2" -> "value2")),
      (
        "app.kubernetes.io/name=spark,app.kubernetes.io/version=3.5.0",
        Map("app.kubernetes.io/name" -> "spark", "app.kubernetes.io/version" -> "3.5.0")
      ),
      (" key1=value1 , key2 = value2 ", Map("key1" -> "value1", "key2" -> "value2"))
    )

    forAll(testCases) { (annotations, expected) =>
      commaSeparatedAnnotationsToMap(annotations) shouldEqual expected
    }
  }

  test("isValidFilePath") {
    import java.lang.reflect.Method

    // Access the private method using reflection
    val method: Method = Config.getClass.getDeclaredMethod("isValidFilePath", classOf[String])
    method.setAccessible(true)

    def callIsValidFilePath(path: String): Boolean = {
      method.invoke(Config, path).asInstanceOf[Boolean]
    }

    val validPaths = Table(
      "path",
      // Absolute paths
      "/absolute/path/to/template.yaml",
      "/home/user/template.yaml",
      "/etc/spark/config.yaml",

      // Relative paths
      "relative/path/to/template.yaml",
      "config/template.yaml",
      "./template.yaml",
      "../config/template.yaml",
      "template.yaml",

      // File URIs
      "file:///absolute/path/to/template.yaml",
      "file:///home/user/template.yaml",
      "FILE:///etc/spark/config.yaml",

      // HTTP/HTTPS URLs
      "http://config-server.example.com/spark-template.yaml",
      "https://config-server.example.com/spark-template.yaml",
      "HTTP://localhost:8080/template.yaml",
      "HTTPS://server.com/config.yaml"
    )

    val invalidPaths = Table(
      "path",
      // Empty string
      "",

      // Invalid protocols
      "ftp://server.com/template.yaml",
      "ssh://server.com/template.yaml",
      "hdfs://namenode/path/to/template.yaml",

      // Malformed URIs
      "://invalid/uri"
    )

    forAll(validPaths) { path =>
      callIsValidFilePath(path) shouldBe true
    }

    forAll(invalidPaths) { path =>
      callIsValidFilePath(path) shouldBe false
    }
  }
}
