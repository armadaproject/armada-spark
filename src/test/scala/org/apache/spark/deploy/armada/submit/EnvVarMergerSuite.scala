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

import k8s.io.api.core.v1.generated.EnvVar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for the environment variable merging logic used in ArmadaClientApplication.
 */
class EnvVarMergerSuite extends AnyFunSuite with Matchers {
  private val aca = new ArmadaClientApplication

  test("mergeEnvVars should merge two non-overlapping sequences") {
    val firstSeq = Seq(
      EnvVar().withName("VAR1").withValue("value1"),
      EnvVar().withName("VAR2").withValue("value2")
    )
    val secondSeq = Seq(
      EnvVar().withName("VAR3").withValue("value3"),
      EnvVar().withName("VAR4").withValue("value4")
    )
    
    val result = aca.mergeEnvVars(firstSeq, secondSeq)
    result should contain theSameElementsAs (firstSeq ++ secondSeq)
  }

  test("mergeEnvVars should override values from first sequence with values from second sequence") {
    val firstSeq = Seq(
      EnvVar().withName("VAR1").withValue("value1"),
      EnvVar().withName("VAR2").withValue("value2"),
      EnvVar().withName("COMMON").withValue("first-value")
    )
    val secondSeq = Seq(
      EnvVar().withName("VAR3").withValue("value3"),
      EnvVar().withName("COMMON").withValue("second-value")
    )
    
    val result = aca.mergeEnvVars(firstSeq, secondSeq)
    result should contain (EnvVar().withName("COMMON").withValue("second-value"))
    result should contain (EnvVar().withName("VAR1").withValue("value1"))
    result should contain (EnvVar().withName("VAR2").withValue("value2"))
    result should contain (EnvVar().withName("VAR3").withValue("value3"))
    result.size shouldBe 4
  }

  test("mergeEnvVars should handle empty first sequence") {
    val emptySeq = Seq.empty[EnvVar]
    val nonEmptySeq = Seq(
      EnvVar().withName("VAR1").withValue("value1"),
      EnvVar().withName("VAR2").withValue("value2")
    )
    
    val result = aca.mergeEnvVars(emptySeq, nonEmptySeq)
    result should contain theSameElementsAs nonEmptySeq
  }

  test("mergeEnvVars should handle empty second sequence") {
    val emptySeq = Seq.empty[EnvVar]
    val nonEmptySeq = Seq(
      EnvVar().withName("VAR1").withValue("value1"),
      EnvVar().withName("VAR2").withValue("value2")
    )
    
    val result = aca.mergeEnvVars(nonEmptySeq, emptySeq)
    result should contain theSameElementsAs nonEmptySeq
  }

  test("mergeEnvVars should handle both sequences empty") {
    val emptySeq = Seq.empty[EnvVar]
    
    val result = aca.mergeEnvVars(emptySeq, emptySeq)
    result shouldBe empty
  }

  test("mergeEnvVars should handle complex environment variables with valueFrom") {
    val source = k8s.io.api.core.v1.generated.EnvVarSource()
      .withFieldRef(
        k8s.io.api.core.v1.generated.ObjectFieldSelector()
          .withApiVersion("v1")
          .withFieldPath("status.podIP")
      )
    
    val firstSeq = Seq(
      EnvVar().withName("VAR1").withValue("value1"),
      EnvVar().withName("POD_IP").withValueFrom(source)
    )
    val secondSeq = Seq(
      EnvVar().withName("VAR2").withValue("value2"),
      EnvVar().withName("POD_IP").withValue("fixed-ip")
    )
    
    val result = aca.mergeEnvVars(firstSeq, secondSeq)
    result should contain (EnvVar().withName("VAR1").withValue("value1"))
    result should contain (EnvVar().withName("VAR2").withValue("value2"))
    result should contain (EnvVar().withName("POD_IP").withValue("fixed-ip"))
    result.size shouldBe 3
  }
}