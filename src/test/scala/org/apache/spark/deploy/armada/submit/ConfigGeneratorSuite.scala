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

import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path, StandardOpenOption}

class ConfigGeneratorSuite extends AnyFunSuite with BeforeAndAfter {
  private val sparkConf = new SparkConf(false)
  private var tempDir: Path = _
  private var sparkConfFile: Path = _
  private var confDir: Path = _
  private val prefix = "testPrefix"
  private val configFileContents =
      """
        |spark.app.name TestApp
        |spark.master localhost
        |""".stripMargin

  before {
    // Create temporary directory
    tempDir = Files.createTempDirectory("spark-test-")
    confDir = Files.createDirectory(tempDir.resolve("conf"))
    // Create spark-defaults.conf inside it
    sparkConfFile = confDir.resolve("spark-defaults.conf")

    // Write sample config to the file
    Files.writeString(sparkConfFile, configFileContents, StandardOpenOption.CREATE)

    sparkConf.set("spark.home", tempDir.toString)

  }
  test("Test annotations") {
    val expectedString = s"Map($prefix/spark-defaults.conf -> $configFileContents)"
    val cg = new ConfigGenerator(prefix, sparkConf)
    val ann = cg.getAnnotations
    assert(ann.toString == expectedString)
  }

  test("Test volumes") {
    val expectedString =
    s"""|name: "$prefix-volume"
        |volumeSource {
        |  downwardAPI {
        |    items {
        |      path: "spark-defaults.conf"
        |      fieldRef {
        |        fieldPath: "metadata.annotations['$prefix/spark-defaults.conf']"
        |      }
        |    }
        |  }
        |}
        |""".stripMargin

    val cg = new ConfigGenerator(prefix, sparkConf)
    val vol = cg.getVolumes
    assert(vol.head.toProtoString == expectedString)
  }

  test("Test volume mounts") {
    val expectedString =
    s"""|name: "$prefix-volume"
        |readOnly: true
        |mountPath: "${ConfigGenerator.REMOTE_CONF_DIR_NAME}"
        |""".stripMargin

    val cg = new ConfigGenerator(prefix, sparkConf)
    val volMounts = cg.getVolumeMounts
    assert(volMounts.head.toProtoString == expectedString)
  }

  after {
    // Clean up
    Files.deleteIfExists(sparkConfFile)
    Files.deleteIfExists(confDir)
    Files.deleteIfExists(tempDir)
  }


}