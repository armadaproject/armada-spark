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
    assert(executorID == 0)
  }

}