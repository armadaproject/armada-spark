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

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.util.Date

import com.auth0.jwt.JWT
import com.auth0.jwt.RegisteredClaims
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.JWTVerificationException
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JwtValidatorSuite extends AnyFunSuite with Matchers {

  private val keyPair = {
    val gen = KeyPairGenerator.getInstance("RSA")
    gen.initialize(2048)
    gen.generateKeyPair()
  }
  private val pub    = keyPair.getPublic.asInstanceOf[RSAPublicKey]
  private val priv   = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]
  private val alg    = Algorithm.RSA256(pub, priv)
  private val issuer = "https://idp.test/"

  private def buildVerifier(audience: String = null) = {
    val b = JWT.require(alg).withIssuer(issuer).withClaimPresence(RegisteredClaims.EXPIRES_AT)
    if (audience != null) b.withAudience(audience)
    b.build()
  }

  test("verifies a fresh token signed with the matching key") {
    val token = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val validator = new JwtValidator(buildVerifier(), issuer, null)
    val jwt       = validator.verify(token)
    jwt.getSubject shouldBe "alice@example.com"
  }

  test("rejects a token with the wrong issuer") {
    val token = JWT
      .create()
      .withIssuer("https://attacker.test/")
      .withSubject("alice@example.com")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val validator = new JwtValidator(buildVerifier(), issuer, null)
    a[JWTVerificationException] should be thrownBy validator.verify(token)
  }

  test("rejects an expired token") {
    val token = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .withExpiresAt(new Date(System.currentTimeMillis() - 60000))
      .sign(alg)

    val validator = new JwtValidator(buildVerifier(), issuer, null)
    a[JWTVerificationException] should be thrownBy validator.verify(token)
  }

  test("rejects a token with no exp claim") {
    val token = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .sign(alg)

    val validator = new JwtValidator(buildVerifier(), issuer, null)
    a[JWTVerificationException] should be thrownBy validator.verify(token)
  }

  test("parseJwksUri extracts jwks_uri from an OIDC discovery doc") {
    val body =
      """{"issuer":"https://idp.test/","jwks_uri":"https://idp.test/jwks","other":"x"}"""
    JwtValidator.parseJwksUri(body) shouldBe "https://idp.test/jwks"
  }

  test("parseJwksUri throws when jwks_uri is missing") {
    val body = """{"issuer":"https://idp.test/"}"""
    an[IllegalStateException] should be thrownBy JwtValidator.parseJwksUri(body)
  }

  test("parseJwksUri throws on malformed discovery JSON") {
    an[IllegalStateException] should be thrownBy JwtValidator.parseJwksUri("not json")
  }

  test("accepts a token whose aud matches the configured audience") {
    val token = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .withAudience("spark-connect")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val verifier  = JWT.require(alg).withIssuer(issuer).withAudience("spark-connect").build()
    val validator = new JwtValidator(verifier, issuer, "spark-connect")
    validator.verify(token).getSubject shouldBe "alice@example.com"
  }

  test("rejects a token whose aud does not match the configured audience") {
    val token = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .withAudience("some-other-app")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val verifier  = JWT.require(alg).withIssuer(issuer).withAudience("spark-connect").build()
    val validator = new JwtValidator(verifier, issuer, "spark-connect")
    a[JWTVerificationException] should be thrownBy validator.verify(token)
  }

  test("rejects a token with no aud when audience is required") {
    val token = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val verifier  = JWT.require(alg).withIssuer(issuer).withAudience("spark-connect").build()
    val validator = new JwtValidator(verifier, issuer, "spark-connect")
    a[JWTVerificationException] should be thrownBy validator.verify(token)
  }

  private def authConf(issuerUrl: String, jwksUrl: String = null): SparkConf = {
    val c = new SparkConf(false)
      .set("spark.armada.connect.owner", "alice@example.com")
      .set("spark.armada.connect.oidc.issuerUrl", issuerUrl)
    if (jwksUrl != null) c.set("spark.armada.connect.oidc.jwksUrl", jwksUrl)
    c
  }

  test("rejects a non-https jwksUrl") {
    an[IllegalStateException] should be thrownBy new JwtValidator(
      AuthConfig.from(authConf(issuer, "http://idp.test/jwks"))
    )
  }

  test("builds with an explicit jwksUrl (no discovery)") {
    noException should be thrownBy new JwtValidator(
      AuthConfig.from(authConf(issuer, "https://idp.test/jwks"))
    )
  }

  test("rejects a non-https issuer when discovery is used") {
    an[IllegalStateException] should be thrownBy new JwtValidator(
      AuthConfig.from(authConf("http://idp.test/"))
    )
  }

  test("rejects a non-https issuer even with an explicit jwksUrl override") {
    an[IllegalStateException] should be thrownBy new JwtValidator(
      AuthConfig.from(authConf("http://idp.test/", "https://idp.test/jwks"))
    )
  }

  // --- networked discovery (fetchJwksUri) against a local HTTP server ---

  /** Start a throwaway loopback HTTP server, run `body` with its base URL, then stop it. */
  private def withServer(routes: PartialFunction[String, (Int, Map[String, String], String)])(
      body: String => Unit
  ): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      new HttpHandler {
        override def handle(ex: HttpExchange): Unit = {
          val (code, headers, payload) =
            routes.applyOrElse(
              ex.getRequestURI.getPath,
              (_: String) => (404, Map.empty[String, String], "")
            )
          headers.foreach { case (k, v) => ex.getResponseHeaders.add(k, v) }
          val bytes = payload.getBytes(StandardCharsets.UTF_8)
          // -1 length for redirects/empty bodies so no content is written.
          ex.sendResponseHeaders(code, if (bytes.isEmpty) -1 else bytes.length.toLong)
          if (bytes.nonEmpty) {
            val os = ex.getResponseBody
            os.write(bytes)
            os.close()
          }
          ex.close()
        }
      }
    )
    server.start()
    try body(s"http://127.0.0.1:${server.getAddress.getPort}")
    finally server.stop(0)
  }

  private val discoveryPath = "/.well-known/openid-configuration"
  private val discoveryDoc = """{"issuer":"https://idp.test/","jwks_uri":"https://idp.test/jwks"}"""

  test("fetchJwksUri returns jwks_uri from a 200 discovery response") {
    withServer({ case `discoveryPath` => (200, Map.empty, discoveryDoc) }) { base =>
      JwtValidator.fetchJwksUri(base + discoveryPath) shouldBe "https://idp.test/jwks"
    }
  }

  test("fetchJwksUri throws on a non-2xx discovery response") {
    withServer({ case `discoveryPath` => (500, Map.empty, "boom") }) { base =>
      an[IllegalStateException] should be thrownBy JwtValidator.fetchJwksUri(base + discoveryPath)
    }
  }

  test("fetchJwksUri follows redirects during discovery") {
    withServer({
      case `discoveryPath`   => (302, Map("Location" -> "/real-discovery"), "")
      case "/real-discovery" => (200, Map.empty, discoveryDoc)
    }) { base =>
      JwtValidator.fetchJwksUri(base + discoveryPath) shouldBe "https://idp.test/jwks"
    }
  }
}
