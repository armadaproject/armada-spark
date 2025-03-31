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

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.mockito.Mockito.when
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path, StandardOpenOption}

class ConfigGeneratorSuite

    extends AnyFunSuite with BeforeAndAfter {
  @Mock
  private var sc: SparkContext = _

  @Mock
  private var env: SparkEnv = _

  @Mock
  private var taskSchedulerImpl: TaskSchedulerImpl = _

  @Mock
  private var rpcEnv: RpcEnv = _

  private val timeout = 10000
  private val sparkConf = new SparkConf(false)
    .set("spark.armada.executor.trackerTimeout", timeout.toString)
  var tempDir: Path = _
  var sparkConfFile: Path = _
  var confDir: Path = _

  before {
    MockitoAnnotations.openMocks(this).close()
    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(env)
    when(taskSchedulerImpl.sc).thenReturn(sc)
    when(env.rpcEnv).thenReturn(rpcEnv)
    when(taskSchedulerImpl.isExecutorAlive("1")).thenReturn(true)

    // Create temporary directory
    tempDir = Files.createTempDirectory("spark-test-")
    confDir = Files.createDirectory(tempDir.resolve("conf"))
    // Create spark-defaults.conf inside it
    sparkConfFile = confDir.resolve("spark-defaults.conf")

    // Write sample config to the file
    val content =
      """
        |spark.app.name TestApp
        |spark.master local[*]
        |""".stripMargin

    Files.writeString(sparkConfFile, content, StandardOpenOption.CREATE)

    sparkConf.set("spark.home", tempDir.toString)

  }
  test("test annotations") {
    val cg = new ConfigGenerator("prefix", sparkConf)
    val ann = cg.getAnnotations
    val annString = ann.toString
    assert(annString == "Map(prefix/spark-defaults.conf -> \nspark.app.name TestApp\nspark.master local[*]")
  }

  test("test volumes") {
    val expectedString =
     """|name: "prefix-volume"
        |volumeSource {
        |  downwardAPI {
        |    items {
        |      path: "spark-defaults.conf"
        |      fieldRef {
        |        fieldPath: "metadata.annotations['prefix/spark-defaults.conf']"
        |      }
        |    }
        |  }
        |}
        |""".stripMargin

    val cg = new ConfigGenerator("prefix", sparkConf)
    val vol = cg.getVolumes
    assert(vol.head.toProtoString == expectedString)
  }

  after {
    // Clean up
    Files.deleteIfExists(sparkConfFile)
    Files.deleteIfExists(confDir)
    Files.deleteIfExists(tempDir)
  }


}