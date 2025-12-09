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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.submit.ArmadaUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeploymentModeHelperSuite extends AnyFunSuite with Matchers {

  test(
    "DeploymentModeHelper.apply: creates StaticCluster for cluster mode without dynamic allocation"
  ) {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.instances", "5")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[StaticCluster]
    helper.getExecutorCount shouldBe 5
    helper.getGangCardinality shouldBe 6
  }

  test(
    "DeploymentModeHelper.apply: creates DynamicCluster for cluster mode with dynamic allocation"
  ) {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[DynamicCluster]
    helper.getExecutorCount shouldBe 2
    helper.getGangCardinality shouldBe 3
  }

  test(
    "DeploymentModeHelper.apply: creates StaticClient for client mode without dynamic allocation"
  ) {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.instances", "3")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[StaticClient]
    helper.getExecutorCount shouldBe 3
    helper.getGangCardinality shouldBe 3
  }

  test(
    "DeploymentModeHelper.apply: creates DynamicClient for client mode with dynamic allocation"
  ) {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "1")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[DynamicClient]
    helper.getExecutorCount shouldBe 1
    helper.getGangCardinality shouldBe 1
  }

  test("getDriverHostName: StaticCluster returns service name from job ID") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")

    val helper           = DeploymentModeHelper(conf)
    val driverJobId      = "test-job-123"
    val expectedHostname = ArmadaUtils.buildServiceNameFromJobId(driverJobId)

    helper.getDriverHostName(driverJobId) shouldBe expectedHostname
    helper.getDriverHostName(driverJobId) shouldBe "armada-test-job-123-0-service-0"
  }

  test("getDriverHostName: DynamicCluster returns service name from job ID") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")

    val helper           = DeploymentModeHelper(conf)
    val driverJobId      = "dynamic-job-456"
    val expectedHostname = ArmadaUtils.buildServiceNameFromJobId(driverJobId)

    helper.getDriverHostName(driverJobId) shouldBe expectedHostname
    helper.getDriverHostName(driverJobId) shouldBe "armada-dynamic-job-456-0-service-0"
  }

  test("getDriverHostName: StaticClient returns spark.driver.host from config") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.driver.host", "my-driver-host.example.com")

    val helper      = DeploymentModeHelper(conf)
    val driverJobId = "client-job-789"

    helper.getDriverHostName(driverJobId) shouldBe "my-driver-host.example.com"
  }

  test("getDriverHostName: DynamicClient returns spark.driver.host from config") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.driver.host", "external-driver.local")

    val helper      = DeploymentModeHelper(conf)
    val driverJobId = "dynamic-client-job-101"

    helper.getDriverHostName(driverJobId) shouldBe "external-driver.local"
  }

  test(
    "getDriverHostName: StaticClient throws IllegalArgumentException when spark.driver.host is not set"
  ) {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "false")

    val helper      = DeploymentModeHelper(conf)
    val driverJobId = "client-job-without-host"

    val exception = intercept[IllegalArgumentException] {
      helper.getDriverHostName(driverJobId)
    }

    exception.getMessage should include("spark.driver.host must be set in client mode")
  }

  test(
    "getDriverHostName: DynamicClient throws IllegalArgumentException when spark.driver.host is not set"
  ) {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")

    val helper      = DeploymentModeHelper(conf)
    val driverJobId = "dynamic-client-job-without-host"

    val exception = intercept[IllegalArgumentException] {
      helper.getDriverHostName(driverJobId)
    }

    exception.getMessage should include("spark.driver.host must be set in client mode")
  }
}
