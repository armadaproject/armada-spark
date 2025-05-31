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
import org.apache.spark.deploy.armada.Config._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer
import scala.sys.process._

class ArmadaClientApplicationSuite extends AnyFunSuite with BeforeAndAfter {
  var sparkConf   = new SparkConf(false)
  val imageName   = "imageName"
  val sparkMaster = "localhost"
  val className   = "testClass"
  val clientArgs: ClientArguments =
    ClientArguments.fromCommandLineArgs(Array("--main-class", className))
  val bindAddress         = "$(SPARK_DRIVER_BIND_ADDRESS)"
  val queue               = "test"
  val nodeUniformityLabel = "nodeUniformity"
  val jobSet              = "job-set"

  before {
    sparkConf = new SparkConf(false)
    sparkConf.set(CONTAINER_IMAGE, imageName)
    sparkConf.set(ARMADA_JOB_QUEUE, queue)
    sparkConf.set(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY, nodeUniformityLabel)
    sparkConf.set(ARMADA_JOB_SET_ID, jobSet)
    sparkConf.set("spark.master", sparkMaster)
  }

  test("Test driver container default values") {
    // Set expected values to defaults
    val mem = DEFAULT_MEM
    val cpu = DEFAULT_CORES
    val defaultValues = Map[String, String](
      "sparkMaster" -> sparkMaster,
      "limitMem"    -> mem,
      "limitCPU"    -> cpu,
      "requestMem"  -> mem,
      "requestCPU"  -> cpu
    )

    val aca             = new ArmadaClientApplication()
    val armadaJobConfig = aca.validateArmadaJobConfig(sparkConf)
    val (driver, _)     = aca.newSparkJobSubmitRequestItems(clientArgs, armadaJobConfig, sparkConf)
    assert(driver.namespace == "default")
    assert(driver.priority == 0)

    val container = driver.getPodSpec.containers.head
    assert(container.getImage == imageName)

    val driverArgs       = getExpectedDriverArgs(defaultValues)
    val argSize          = driverArgs.split("\n").length
    val driverArgsString = container.args.take(argSize).mkString("\n")
    assert(driverArgsString == driverArgs)

    val driverConfList         = container.args.drop(argSize).filter(_ != "--conf").sorted.toList
    val driverConfExpectedList = getExpectedDriverConf(defaultValues).split("\n").sorted.toList
    assert(driverConfList == driverConfExpectedList)
    val driverPortString = container.ports.head.toProtoString
    assert(driverPortString == getExpectedDriverPort)

    // Filter out service name because it contains a random suffix
    val driverEnvString = container.env
      .filter(_.getName != "ARMADA_SPARK_DRIVER_SERVICE_NAME")
      .map(_.toProtoString)
      .mkString
    assert(driverEnvString == getExpectedDriverEnv)

    // Test service name by filter out random suffix
    val driverServicePrefix =
      container.env.filter(_.getName == "ARMADA_SPARK_DRIVER_SERVICE_NAME").head.getValue
    val validPrefix = "armada-spark-driver-.....".r
    assert(driverServicePrefix.matches(validPrefix.regex))

    val driverResourcesString = container.resources.get.toProtoString
    assert(driverResourcesString == getExpectedResources(defaultValues))
  }

  test("Test driver container non-default values") {
    // Change expected values from defaults
    val mem       = "20Gi"
    val cpu       = "2"
    val namespace = "namespace"
    val priority  = 10.0
    sparkConf.set(ARMADA_DRIVER_LIMIT_MEMORY, mem)
    sparkConf.set(ARMADA_DRIVER_LIMIT_CORES, cpu)
    sparkConf.set(ARMADA_DRIVER_REQUEST_MEMORY, mem)
    sparkConf.set(ARMADA_DRIVER_REQUEST_CORES, cpu)
    sparkConf.set(ARMADA_SPARK_JOB_NAMESPACE, namespace)
    sparkConf.set(ARMADA_SPARK_JOB_PRIORITY, priority)
    val nonDefaultValues = Map[String, String](
      "limitMem"   -> mem,
      "limitCPU"   -> cpu,
      "requestMem" -> mem,
      "requestCPU" -> cpu
    )

    val aca             = new ArmadaClientApplication()
    val armadaJobConfig = aca.validateArmadaJobConfig(sparkConf)
    val (driver, _)     = aca.newSparkJobSubmitRequestItems(clientArgs, armadaJobConfig, sparkConf)
    assert(driver.namespace == namespace)
    assert(driver.priority == priority)

    val container             = driver.getPodSpec.containers.head
    val driverResourcesString = container.resources.get.toProtoString
    assert(driverResourcesString == getExpectedResources(nonDefaultValues))
  }

  private def getExpectedDriverArgs(valueMap: Map[String, String]) = {
    s"""|driver
        |--verbose
        |--master
        |${valueMap("sparkMaster")}
        |--class
        |$className""".stripMargin
  }

  private def getExpectedDriverConf(valueMap: Map[String, String]) = {
    s"""|spark.driver.port=7078
        |spark.driver.host=$bindAddress
        |spark.armada.scheduling.nodeUniformity=$nodeUniformityLabel
        |spark.armada.queue=$queue
        |spark.master=${valueMap("sparkMaster")}
        |spark.armada.container.image=$imageName
        |spark.armada.jobSetId=$jobSet""".stripMargin
  }

  private def getExpectedDriverPort = {
    s"""|name: "driver"
        |containerPort: 7078
        |""".stripMargin
  }

  private def getExpectedDriverEnv = {
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
        |""".stripMargin

  }
  private def getExpectedResources(valueMap: Map[String, String]) = {
    s"""|limits {
        |  key: "memory"
        |  value {
        |    string: "${valueMap("limitMem")}"
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
        |  key: "cpu"
        |  value {
        |    string: "${valueMap("requestCPU")}"
        |  }
        |}
        |""".stripMargin
  }

  test("Test executor container default values") {
    // Set expected values to defaults
    val mem      = DEFAULT_MEM
    val cpu      = DEFAULT_CORES
    val appId    = "armada-spark-app-id"
    val envMem   = DEFAULT_SPARK_EXECUTOR_MEMORY
    val envCores = DEFAULT_SPARK_EXECUTOR_CORES
    val defaultValues = Map[String, String](
      "limitMem"   -> mem,
      "limitCPU"   -> cpu,
      "requestMem" -> mem,
      "requestCPU" -> cpu,
      "appId"      -> appId,
      "envMem"     -> envMem,
      "envCores"   -> envCores
    )

    val aca             = new ArmadaClientApplication()
    val armadaJobConfig = aca.validateArmadaJobConfig(sparkConf)
    val (_, executors)  = aca.newSparkJobSubmitRequestItems(clientArgs, armadaJobConfig, sparkConf)
    assert(executors.size == 2)

    val executor = executors.head
    assert(executor.namespace == "default")
    assert(executor.priority == 0)

    val container = executor.getPodSpec.containers.filter(_.getName == "executor").head

    assert(container.getImage == imageName)

    // filter out driver url, (contains random suffix)
    val executorEnvString =
      container.env.filter(_.getName != "SPARK_DRIVER_URL").map(_.toProtoString).mkString
    assert(executorEnvString == getExpectedExecutorEnv(defaultValues))

    val driverUrl = container.env.filter(_.getName == "SPARK_DRIVER_URL").head.getValue
    val validUrl  = "spark://CoarseGrainedScheduler@armada-spark-driver-.....:7078".r
    assert(driverUrl.matches(validUrl.regex))
    val executorResourcesString = container.resources.get.toProtoString
    assert(executorResourcesString == getExpectedResources(defaultValues))
  }

  test("Test executor container non-default values") {
    // Change expected values from defaults
    val mem           = "10Gi"
    val cpu           = "10"
    val appId         = "nonDefault"
    val envMem        = "10g"
    val instanceCount = 4
    val namespace     = "namespace"
    val priority      = 10.0
    val driverPrefix  = "driverPrefix-"
    sparkConf.set(ARMADA_EXECUTOR_LIMIT_MEMORY, mem)
    sparkConf.set(ARMADA_EXECUTOR_LIMIT_CORES, cpu)
    sparkConf.set(ARMADA_EXECUTOR_REQUEST_MEMORY, mem)
    sparkConf.set(ARMADA_EXECUTOR_REQUEST_CORES, cpu)
    sparkConf.set(ARMADA_SPARK_JOB_NAMESPACE, namespace)
    sparkConf.set(ARMADA_SPARK_JOB_PRIORITY, priority)
    sparkConf.set(SPARK_DRIVER_SERVICE_NAME_PREFIX, driverPrefix)

    sparkConf.set("spark.app.id", appId)
    sparkConf.set("spark.executor.memory", envMem)
    sparkConf.set("spark.executor.cores", cpu)
    sparkConf.set("spark.executor.instances", instanceCount.toString)

    val nonDefaultValues = Map[String, String](
      "limitMem"   -> mem,
      "limitCPU"   -> cpu,
      "requestMem" -> mem,
      "requestCPU" -> cpu,
      "appId"      -> appId,
      "envMem"     -> envMem,
      "envCores"   -> cpu
    )

    val aca             = new ArmadaClientApplication()
    val armadaJobConfig = aca.validateArmadaJobConfig(sparkConf)
    val (_, executors)  = aca.newSparkJobSubmitRequestItems(clientArgs, armadaJobConfig, sparkConf)
    assert(executors.size == instanceCount)

    val executor = executors.head
    assert(executor.namespace == namespace)
    assert(executor.priority == priority)

    val container = executor.getPodSpec.containers.filter(_.getName == "executor").head
    val executorEnvString =
      container.env.filter(_.getName != "SPARK_DRIVER_URL").map(_.toProtoString).mkString
    assert(executorEnvString == getExpectedExecutorEnv(nonDefaultValues))

    val driverUrl = container.env.filter(_.getName == "SPARK_DRIVER_URL").head.getValue
    val validUrl  = s"spark://CoarseGrainedScheduler@$driverPrefix.....:7078".r
    assert(driverUrl.matches(validUrl.regex))

    val executorResourcesString = container.resources.get.toProtoString
    assert(executorResourcesString == getExpectedResources(nonDefaultValues))
  }

  private def getExpectedExecutorEnv(valueMap: Map[String, String]) = {
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
        |value: "${valueMap("appId")}"
        |name: "SPARK_EXECUTOR_CORES"
        |value: "${valueMap("envCores")}"
        |name: "SPARK_EXECUTOR_MEMORY"
        |value: "${valueMap("envMem")}"
        |name: "SPARK_EXECUTOR_POD_IP"
        |valueFrom {
        |  fieldRef {
        |    apiVersion: "v1"
        |    fieldPath: "status.podIP"
        |  }
        |}
        |name: "ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL"
        |value: "$nodeUniformityLabel"
        |name: "SPARK_JAVA_OPT_0"
        |value: "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
        |""".stripMargin
  }

  test("Confirm initContainer sh command succeeds with server") {
    // start server
    val serverPort    = "54525"
    val serverCommand = Seq("nc", "-l", serverPort)
    val server        = Process.apply(serverCommand).run

    // start client
    val containerCommand = Seq("sh", "-c", Utils.initContainerCommand)
    try {
      val client = Process
        .apply(
          containerCommand,
          None,
          ("SPARK_EXECUTOR_CONNECTION_TIMEOUT", "5"),
          ("SPARK_DRIVER_HOST", "localhost"),
          ("SPARK_DRIVER_PORT", serverPort)
        )
        .run
      assert(client.exitValue == 0)
    } finally {
      server.destroy()
    }
  }

  test("Confirm initContainer sh command fails with no server") {
    val serverPort       = "54526"
    val timeout          = "5"
    val containerCommand = Seq("sh", "-c", Utils.initContainerCommand)
    val client = Process.apply(
      containerCommand,
      None,
      ("SPARK_EXECUTOR_CONNECTION_TIMEOUT", timeout),
      ("SPARK_DRIVER_HOST", "localhost"),
      ("SPARK_DRIVER_PORT", serverPort)
    )
    val stringBuffer = ListBuffer.empty[String]
    assertThrows[RuntimeException](client.lineStream.foreach(stringBuffer += _))
    val finalList = stringBuffer.toList
    assert(finalList.contains(s"Timeout waiting for driver after ${timeout}s"))
  }
}
