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
package io.armadaproject.spark.connect.auth

import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AuthConfigSuite extends AnyFunSuite with Matchers {

  private def conf(pairs: (String, String)*): SparkConf = {
    val c = new SparkConf(false)
    pairs.foreach { case (k, v) => c.set(k, v) }
    c
  }

  test("throws when owner is missing") {
    an[IllegalStateException] should be thrownBy AuthConfig.from(
      conf("spark.armada.connect.oidc.issuerUrl" -> "https://idp.test/")
    )
  }

  test("throws when issuer is missing") {
    an[IllegalStateException] should be thrownBy AuthConfig.from(
      conf("spark.armada.connect.owner" -> "alice@example.com")
    )
  }

  test("throws when issuer is blank") {
    an[IllegalStateException] should be thrownBy AuthConfig.from(
      conf(
        "spark.armada.connect.owner"          -> "alice@example.com",
        "spark.armada.connect.oidc.issuerUrl" -> "  "
      )
    )
  }

  test("trims values and treats blank optionals as unset") {
    val cfg = AuthConfig.from(
      conf(
        "spark.armada.connect.owner"          -> "  alice@example.com  ",
        "spark.armada.connect.oidc.issuerUrl" -> "  https://idp.test/  ",
        "spark.armada.connect.oidc.jwksUrl"   -> "   ",
        "spark.armada.connect.oidc.audience"  -> "  spark-connect  "
      )
    )
    cfg.owner shouldBe "alice@example.com"
    cfg.issuerUrl shouldBe "https://idp.test/"
    cfg.jwksUrl shouldBe null
    cfg.audience shouldBe "spark-connect"
  }

  test("leaves optionals null when unset") {
    val cfg = AuthConfig.from(
      conf(
        "spark.armada.connect.owner"          -> "alice@example.com",
        "spark.armada.connect.oidc.issuerUrl" -> "https://idp.test/"
      )
    )
    cfg.jwksUrl shouldBe null
    cfg.audience shouldBe null
  }
}
