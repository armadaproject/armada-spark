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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OAuthSidecarBuilderSuite extends AnyFunSuite with Matchers {

  private def baseOAuthConf(): SparkConf = {
    new SparkConf()
      .set(ARMADA_OAUTH_ENABLED.key, "true")
      .set(ARMADA_OAUTH_CLIENT_ID.key, "test-client")
      .set(ARMADA_OAUTH_CLIENT_SECRET.key, "test-secret")
      .set(ARMADA_OAUTH_ISSUER_URL.key, "https://issuer.example.com")
  }

  test("buildOAuthSidecar returns None when OAuth is disabled") {
    val conf = new SparkConf().set(ARMADA_OAUTH_ENABLED.key, "false")
    OAuthSidecarBuilder.buildOAuthSidecar(conf) shouldBe None
  }

  test("buildOAuthSidecar returns container with valid config") {
    val conf   = baseOAuthConf()
    val result = OAuthSidecarBuilder.buildOAuthSidecar(conf)

    result shouldBe defined
    val container = result.get
    container.name shouldBe Some("oauth")
    container.restartPolicy shouldBe Some("Always")
    container.livenessProbe shouldBe defined
    container.ports should have size 1
    container.ports.head.containerPort shouldBe Some(4180)
  }

  test("buildOAuthSidecar uses custom proxy port") {
    val conf      = baseOAuthConf().set(ARMADA_OAUTH_PROXY_PORT.key, "8080")
    val container = OAuthSidecarBuilder.buildOAuthSidecar(conf).get

    container.ports.head.containerPort shouldBe Some(8080)
  }

  test("buildOAuthSidecar fails without client ID") {
    val conf = new SparkConf()
      .set(ARMADA_OAUTH_ENABLED.key, "true")
      .set(ARMADA_OAUTH_CLIENT_SECRET.key, "secret")
      .set(ARMADA_OAUTH_ISSUER_URL.key, "https://issuer.example.com")

    val ex = intercept[IllegalArgumentException] {
      OAuthSidecarBuilder.buildOAuthSidecar(conf)
    }
    ex.getMessage should include("clientId")
  }

  test("buildOAuthSidecar fails without client secret") {
    val conf = new SparkConf()
      .set(ARMADA_OAUTH_ENABLED.key, "true")
      .set(ARMADA_OAUTH_CLIENT_ID.key, "client")
      .set(ARMADA_OAUTH_ISSUER_URL.key, "https://issuer.example.com")

    val ex = intercept[IllegalArgumentException] {
      OAuthSidecarBuilder.buildOAuthSidecar(conf)
    }
    ex.getMessage should include("clientSecret")
  }

  test("buildOAuthSidecar fails without issuer URL when discovery enabled") {
    val conf = new SparkConf()
      .set(ARMADA_OAUTH_ENABLED.key, "true")
      .set(ARMADA_OAUTH_CLIENT_ID.key, "client")
      .set(ARMADA_OAUTH_CLIENT_SECRET.key, "secret")

    val ex = intercept[IllegalArgumentException] {
      OAuthSidecarBuilder.buildOAuthSidecar(conf)
    }
    ex.getMessage should include("issuerUrl")
  }

  test("buildOAuthSidecar requires explicit endpoints when discovery disabled") {
    val conf = new SparkConf()
      .set(ARMADA_OAUTH_ENABLED.key, "true")
      .set(ARMADA_OAUTH_CLIENT_ID.key, "client")
      .set(ARMADA_OAUTH_CLIENT_SECRET.key, "secret")
      .set(ARMADA_OAUTH_SKIP_PROVIDER_DISCOVERY.key, "true")

    val ex = intercept[IllegalArgumentException] {
      OAuthSidecarBuilder.buildOAuthSidecar(conf)
    }
    ex.getMessage should include("loginUrl")
  }

  test("buildOAuthSidecar accepts K8s secret reference for client secret") {
    val conf = new SparkConf()
      .set(ARMADA_OAUTH_ENABLED.key, "true")
      .set(ARMADA_OAUTH_CLIENT_ID.key, "client")
      .set(ARMADA_OAUTH_CLIENT_SECRET_K8S.key, "my-oauth-secret")
      .set(ARMADA_OAUTH_ISSUER_URL.key, "https://issuer.example.com")

    val result = OAuthSidecarBuilder.buildOAuthSidecar(conf)
    result shouldBe defined

    val envVars         = result.get.env
    val clientSecretEnv = envVars.find(_.name.contains("OAUTH2_PROXY_CLIENT_SECRET"))
    clientSecretEnv shouldBe defined
    clientSecretEnv.get.valueFrom shouldBe defined
  }
}
