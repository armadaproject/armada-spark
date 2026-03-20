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
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

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
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[DynamicClient]
    helper.getExecutorCount shouldBe 2
    helper.getGangCardinality shouldBe 2
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
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "1")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

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
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
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
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

    val helper      = DeploymentModeHelper(conf)
    val driverJobId = "dynamic-client-job-without-host"

    val exception = intercept[IllegalArgumentException] {
      helper.getDriverHostName(driverJobId)
    }

    exception.getMessage should include("spark.driver.host must be set in client mode")
  }

  // ========================================================================
  // Gang attribute lifecycle tests
  // ========================================================================

  test("DynamicClient: captureGangAttributes stores values in SparkConf") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    val attributes = Map(
      "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "topology.kubernetes.io/zone",
      "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "us-east-1a"
    )

    helper.captureGangAttributes(attributes)

    conf.getOption("spark.armada.internal.gangNodeLabelName") shouldBe
      Some("topology.kubernetes.io/zone")
    conf.getOption("spark.armada.internal.gangNodeLabelValue") shouldBe
      Some("us-east-1a")
  }

  test("DynamicClient: captureGangAttributes only captures once") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)

    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "zone-label",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "zone-a"
      )
    )
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "zone-label",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "zone-b"
      )
    )

    conf.getOption("spark.armada.internal.gangNodeLabelValue") shouldBe Some("zone-a")
  }

  test("DynamicClient: captureGangAttributes ignores empty label name") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "some-value"
      )
    )

    conf.getOption("spark.armada.internal.gangNodeLabelName") shouldBe None
  }

  test("DynamicClient: captureGangAttributes ignores empty label value") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "topology.kubernetes.io/zone",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> ""
      )
    )

    conf.getOption("spark.armada.internal.gangNodeLabelName") shouldBe None
  }

  test("DynamicClient: captureGangAttributes ignores missing attributes") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.captureGangAttributes(Map("UNRELATED_KEY" -> "value"))

    conf.getOption("spark.armada.internal.gangNodeLabelName") shouldBe None
  }

  test("DynamicClient: isReadyToAllocateMore returns false before gang attributes captured") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.isReadyToAllocateMore shouldBe false
  }

  test("DynamicClient: isReadyToAllocateMore returns true after gang attributes captured") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "topology.kubernetes.io/zone",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "us-east-1a"
      )
    )

    helper.isReadyToAllocateMore shouldBe true
  }

  test("DynamicCluster: isReadyToAllocateMore always returns true") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

    val helper = DeploymentModeHelper(conf)
    helper.isReadyToAllocateMore shouldBe true
  }

  test("DynamicClient: getGangNodeSelector returns empty map before capture") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.getGangNodeSelector shouldBe Map.empty
  }

  test("DynamicClient: getGangNodeSelector returns captured values") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "topology.kubernetes.io/zone",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "us-east-1a"
      )
    )

    helper.getGangNodeSelector shouldBe Map("topology.kubernetes.io/zone" -> "us-east-1a")
  }

  test("DynamicClient: getGangCardinality returns initialExecutors before capture") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "3")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.getGangCardinality shouldBe 3
  }

  test("DynamicClient: getGangCardinality returns 0 after capture") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "topology.kubernetes.io/zone",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "us-east-1a"
      )
    )

    helper.getGangCardinality shouldBe 0
  }

  test("StaticClient: gang methods return defaults") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.instances", "3")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.isReadyToAllocateMore shouldBe true
    helper.getGangNodeSelector shouldBe Map.empty
  }

  test("StaticCluster: gang methods return defaults") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.instances", "5")

    val helper = DeploymentModeHelper(conf)
    helper.isReadyToAllocateMore shouldBe true
    helper.getGangNodeSelector shouldBe Map.empty
  }

  test(
    "DynamicCluster: allows initialExecutors = 1 in dynamic cluster mode"
  ) {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "1")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[DynamicCluster]
    // initialExecutors defaults to minExecutors=1, cardinality = 1 + 1 (driver) = 2
    helper.getGangCardinality shouldBe 2
    helper.getExecutorCount shouldBe 1
  }

  test("DynamicClient: allows minExecutors = 0 when initialExecutors >= 2") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[DynamicClient]
    helper.getExecutorCount shouldBe 0
  }

  test("DynamicClient: throws when initialExecutors < 2") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "1")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val exception = intercept[IllegalArgumentException] {
      DeploymentModeHelper(conf)
    }
    exception.getMessage should include("initialExecutors must be >= 2")
  }

  test("DynamicClient: throws when nodeUniformity not configured") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.driver.host", "localhost")

    val exception = intercept[IllegalArgumentException] {
      DeploymentModeHelper(conf)
    }
    exception.getMessage should include("nodeUniformity")
  }

  test("DynamicClient: minExecutors and initialExecutors are independent") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")
      .set("spark.dynamicAllocation.initialExecutors", "5")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.getExecutorCount shouldBe 2
    helper.getGangCardinality shouldBe 5
  }

  // ========================================================================
  // Scale-from-zero scenario tests
  // ========================================================================

  test("DynamicClient: gang cardinality is 0 after capture even with minExecutors=0") {
    // Simulates the post-bootstrap state: gang attributes captured,
    // then Spark scaled down to 0. Next scale-up should use node selector, not gang.
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)

    // Before capture: gang cardinality = initialExecutors
    helper.getGangCardinality shouldBe 2

    // Simulate initial batch registering and sending gang attributes
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "topology.kubernetes.io/zone",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "us-east-1a"
      )
    )

    // After capture: gang cardinality = 0, node selector carries placement
    helper.getGangCardinality shouldBe 0
    helper.getGangNodeSelector shouldBe Map("topology.kubernetes.io/zone" -> "us-east-1a")
    helper.isReadyToAllocateMore shouldBe true
  }

  test("DynamicClient: getExecutorCount returns 0 when minExecutors=0") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.getExecutorCount shouldBe 0
  }

  test("DynamicClient: isReadyToAllocateMore blocks before gang capture") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "3")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.isReadyToAllocateMore shouldBe false
  }

  test("DynamicClient: minExecutors=0 with high initialExecutors") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "10")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")
      .set("spark.driver.host", "localhost")

    val helper = DeploymentModeHelper(conf)
    helper.getExecutorCount shouldBe 0
    helper.getGangCardinality shouldBe 10
  }

  // ========================================================================
  // DynamicCluster initialExecutors tests
  // ========================================================================

  test("DynamicCluster: throws when nodeUniformity not configured") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")

    val exception = intercept[IllegalArgumentException] {
      DeploymentModeHelper(conf)
    }
    exception.getMessage should include("nodeUniformity")
  }

  test("DynamicCluster: throws when initialExecutors < 1") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "0")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

    val exception = intercept[IllegalArgumentException] {
      DeploymentModeHelper(conf)
    }
    exception.getMessage should include("initialExecutors must be >= 1")
  }

  test("DynamicCluster: getExecutorCount returns initialExecutors") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "3")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

    val helper = DeploymentModeHelper(conf)
    helper.getExecutorCount shouldBe 3
  }

  test("DynamicCluster: getGangCardinality includes driver") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

    val helper = DeploymentModeHelper(conf)
    // cardinality = 2 executors + 1 driver = 3
    helper.getGangCardinality shouldBe 3
  }

  test("DynamicCluster: getGangCardinality returns 0 after capture") {
    val conf = new SparkConf(false)
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.initialExecutors", "2")
      .set("spark.armada.scheduling.nodeUniformity", "armada-spark")

    val helper = DeploymentModeHelper(conf)
    helper.captureGangAttributes(
      Map(
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME"  -> "topology.kubernetes.io/zone",
        "ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE" -> "us-east-1a"
      )
    )

    helper.getGangCardinality shouldBe 0
  }
}
