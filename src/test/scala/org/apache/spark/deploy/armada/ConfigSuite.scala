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
import Config._

class ConfigSuite
 extends AnyFunSuite {
  test("testClusterSelectorsValidator") {
    case class TestCase(
      testSelectors: String,
      expectedValid: Boolean,
      name: String)

    val testCases = List[TestCase](
      // Valid cases
      TestCase("", true, "empty case"),
      TestCase("a=1", true, "One character long name and value"),
      TestCase("armada-spark=true", true, "One valid selector"),
      TestCase("armada-spark=true,spark-cluster-name=001", true, "Two valid selectors"),
      TestCase("armada-spark=false,name=spark-cluster-001,a=1,b=2,c=3", true, "Several valid selectors"),
      TestCase("a" * 63 + "=" + "b" * 63, true, "Selector name & value length limit of 63"),
      TestCase("a" * 30 + "-._" + "b" * 30 + "=b", true,
        "Selector name length limit of 63 with valid non-alphanumeric chars"),
      // Invalid cases
      TestCase("a", false, "key but no value"),
      TestCase("a=", false, "key & = but no value"),
      TestCase("=b", false, "value & = but no key"),
      TestCase("=", false, "just = and no key or value"),
      TestCase("_armada=a", false, "Selector labels must start with an alphanumeric character."),
      TestCase("armada_=b", false, "Selector labels must end with an alphanumeric character."),
      TestCase("armada=_armada", false, "Selector values must start with an alphanumeric character."),
      TestCase("armada=armada_", false, "Selector values must end with an alphanumeric character."),
      TestCase("#@armada-spark=true,spark-cluster-name=spark-cluster-001", false, "Illegal characters: # @"),
      TestCase("a" * 64 + "=b", false, "Selector names must be 63 characters or less"),
      TestCase("armada=" + ("b" * 64), false, "Selector values must be 63 characters or less"),
    )

    for (tc <- testCases) {
        val result = Config.selectorsValidator(tc.testSelectors)
        assert(result == tc.expectedValid, s"test name: '${tc.name}', test value: '${tc.testSelectors}'")
    }
  }

  test("defaultClusterSelectors") {
    val conf = new SparkConf(true)
    assert(conf.get(ARMADA_CLUSTER_SELECTORS) == DEFAULT_CLUSTER_SELECTORS)
  }
}
