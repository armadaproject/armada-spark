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

import k8s.io.api.core.v1.generated.VolumeMount
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.internal.config.DRIVER_CORES


class ArmadaClientApplicationSuite extends AnyFunSuite with BeforeAndAfter {
  val sparkConf = new SparkConf(false)
  val imageName = "imageName"
  val sparkMaster = "localhost"
  val className = "testClass"
  val driverServiceName = "driverService"
  val bindAddress = "$(SPARK_DRIVER_BIND_ADDRESS)"
  val executorID = 0
  before {
    sparkConf.set(CONTAINER_IMAGE, imageName)
    sparkConf.set("spark.master", sparkMaster)
  }

  test("Test get driver container default values") {
    val mem = DEFAULT_MEM
    val storage = DEFAULT_STORAGE
    val cpu = DEFAULT_CORES
    val defaultValues = Map[String, String](
      "sparkMaster" -> sparkMaster,
      "limitMem" -> mem,
      "limitStorage" -> storage,
      "limitCPU" -> cpu,
      "requestMem" -> mem,
      "requestStorage" -> storage,
      "requestCPU" -> cpu)


    val aca = new ArmadaClientApplication()
    val container = aca.getDriverContainer(driverServiceName,
      ClientArguments.fromCommandLineArgs(Array("--main-class", className)), sparkConf, Seq(new VolumeMount))

    val driverArgsString = container.args.mkString("\n")
    assert(driverArgsString == getDriverArgs(defaultValues))

    val driverPortString = container.ports.head.toProtoString
    assert(driverPortString == getDriverPort(defaultValues))

    val driverEnvString = container.env.map(_.toProtoString).mkString
    assert(driverEnvString == getDriverEnv(defaultValues))

    val driverResourcesString = container.resources.get.toProtoString
    assert(driverResourcesString == getResources(defaultValues))
  }

  test("Test get driver container non-default values") {
    val remoteMaster = "remoteMaster"
    val mem = "2Gi"
    val storage = "1Gi"
    val cpu = "2"
    sparkConf.set(ARMADA_REMOTE_MASTER, remoteMaster)
    sparkConf.set(ARMADA_DRIVER_LIMIT_MEMORY, mem)
    sparkConf.set(ARMADA_DRIVER_LIMIT_EPHEMERAL_STORAGE, storage)
    sparkConf.set(ARMADA_DRIVER_LIMIT_CORES, cpu)
    sparkConf.set(ARMADA_DRIVER_REQUEST_MEMORY, mem)
    sparkConf.set(ARMADA_DRIVER_REQUEST_EPHEMERAL_STORAGE, storage)
    sparkConf.set(ARMADA_DRIVER_REQUEST_CORES, cpu)
    val nonDefaultValues = Map[String, String](
      "sparkMaster" -> remoteMaster,
      "limitMem" -> mem,
      "limitStorage" -> storage,
      "limitCPU" -> cpu,
      "requestMem" -> mem,
      "requestStorage" -> storage,
      "requestCPU" -> cpu)


    val aca = new ArmadaClientApplication()
    val container = aca.getDriverContainer(driverServiceName,
      ClientArguments.fromCommandLineArgs(Array("--main-class", className)), sparkConf, Seq(new VolumeMount))

    val driverResourcesString = container.resources.get.toProtoString
    assert(driverResourcesString == getResources(nonDefaultValues))
  }

  private def getDriverArgs(valueMap: Map[String, String]) = {
    s"""|driver
        |--verbose
        |--class
        |$className
        |--master
        |${valueMap("sparkMaster")}
        |--conf
        |spark.driver.port=7078
        |--conf
        |spark.driver.host=$bindAddress
        |--conf
        |spark.master=${valueMap("sparkMaster")}
        |--conf
        |spark.armada.container.image=$imageName""".stripMargin
  }

  private def getDriverPort(valueMap: Map[String, String]) = {
    s"""|name: "armada-spark"
        |hostPort: 0
        |containerPort: 7078
        |""".stripMargin
  }

  private def getDriverEnv(valueMap: Map[String, String]) = {
    s"""|name: "SPARK_DRIVER_BIND_ADDRESS"
        |valueFrom {
        |  fieldRef {
        |    apiVersion: "v1"
        |    fieldPath: "status.podIP"
        |  }
        |}
        |name: "SPARK_CONF_DIR"
        |value: "/opt/spark/conf"
        |name: "EXTERNAL_CLUSTER_SUPPORT_ENABLED"
        |value: "true"
        |name: "ARMADA_SPARK_DRIVER_SERVICE_NAME"
        |value: "$driverServiceName"
        |""".stripMargin

  }
  private def getResources(valueMap: Map[String, String]) = {
    s"""|limits {
        |  key: "memory"
        |  value {
        |    string: "${valueMap("limitMem")}"
        |  }
        |}
        |limits {
        |  key: "ephemeral-storage"
        |  value {
        |    string: "${valueMap("limitStorage")}"
        |  }
        |}
        |limits {
        |  key: "cpu"
        |  value {
        |    string: "${valueMap("limitCPU")}"
        |  }
        |}
        |requests {
        |  key: "memory"
        |  value {
        |    string: "${valueMap("requestMem")}"
        |  }
        |}
        |requests {
        |  key: "ephemeral-storage"
        |  value {
        |    string: "${valueMap("requestStorage")}"
        |  }
        |}
        |requests {
        |  key: "cpu"
        |  value {
        |    string: "${valueMap("requestCPU")}"
        |  }
        |}
        |""".stripMargin
  }

  test("Test default executor container") {
    val mem = DEFAULT_MEM
    val storage = DEFAULT_STORAGE
    val cpu = DEFAULT_CORES
    val defaultValues = Map[String, String](
      "limitMem" -> mem,
      "limitStorage" -> storage,
      "limitCPU" -> cpu,
      "requestMem" -> mem,
      "requestStorage" -> storage,
      "requestCPU" -> cpu)


    val aca = new ArmadaClientApplication()
    val container = aca.getExecutorContainer(executorID, driverServiceName, sparkConf)

    val driverEnvString = container.env.map(_.toProtoString).mkString
    assert(driverEnvString == getExecutorEnv(defaultValues))

    val driverResourcesString = container.resources.get.toProtoString
    assert(driverResourcesString == getResources(defaultValues))
  }

  private def getExecutorEnv(valueMap: Map[String, String]) = {
    s"""|name: "SPARK_EXECUTOR_ID"
        |value: "0"
        |name: "SPARK_RESOURCE_PROFILE_ID"
        |value: "0"
        |name: "SPARK_EXECUTOR_POD_NAME"
        |valueFrom {
        |  fieldRef {
        |    apiVersion: "v1"
        |    fieldPath: "metadata.name"
        |  }
        |}
        |name: "SPARK_APPLICATION_ID"
        |value: "armada-spark-app-id"
        |name: "SPARK_EXECUTOR_CORES"
        |value: "1"
        |name: "SPARK_EXECUTOR_MEMORY"
        |value: "1g"
        |name: "SPARK_DRIVER_URL"
        |value: "spark://CoarseGrainedScheduler@driverService:7078"
        |name: "SPARK_EXECUTOR_POD_IP"
        |valueFrom {
        |  fieldRef {
        |    apiVersion: "v1"
        |    fieldPath: "status.podIP"
        |  }
        |}
        |name: "ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL"
        |value: "armada-spark"
        |name: "SPARK_JAVA_OPT_0"
        |value: "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
        |""".stripMargin
    }

}