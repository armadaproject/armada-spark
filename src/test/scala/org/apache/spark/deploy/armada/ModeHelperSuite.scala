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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ModeHelperSuite extends AnyFunSuite with Matchers {

  test("ModeHelper.apply: creates StaticCluster for cluster mode without dynamic allocation") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.instances", "5")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[StaticCluster]
    helper.getExecutorCount shouldBe 5
    helper.getGangCardinality shouldBe 6
  }

  test("ModeHelper.apply: creates DynamicCluster for cluster mode with dynamic allocation") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "2")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[DynamicCluster]
    helper.getExecutorCount shouldBe 2
    helper.getGangCardinality shouldBe 3
  }

  test("ModeHelper.apply: creates StaticClient for client mode without dynamic allocation") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.instances", "3")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[StaticClient]
    helper.getExecutorCount shouldBe 3
    helper.getGangCardinality shouldBe 3
  }

  test("ModeHelper.apply: creates DynamicClient for client mode with dynamic allocation") {
    val conf = new SparkConf()
      .set("spark.submit.deployMode", "client")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "1")

    val helper = DeploymentModeHelper(conf)
    helper shouldBe a[DynamicClient]
    helper.getExecutorCount shouldBe 1
    helper.getGangCardinality shouldBe 1
  }
}
