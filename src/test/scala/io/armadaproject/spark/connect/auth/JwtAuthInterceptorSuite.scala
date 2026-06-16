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

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.util.Date

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.grpc.{Metadata, ServerCall, ServerCallHandler, Status}
import org.apache.spark.SparkConf
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JwtAuthInterceptorSuite extends AnyFunSuite with Matchers {

  private val keyPair = {
    val gen = KeyPairGenerator.getInstance("RSA")
    gen.initialize(2048)
    gen.generateKeyPair()
  }
  private val pub    = keyPair.getPublic.asInstanceOf[RSAPublicKey]
  private val priv   = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]
  private val alg    = Algorithm.RSA256(pub, priv)
  private val issuer = "https://idp.test/"

  private def verifierFor()  = JWT.require(alg).withIssuer(issuer).build()
  private def validatorFor() = new JwtValidator(verifierFor(), issuer, null)

  private def tokenFor(subject: String) =
    JWT
      .create()
      .withIssuer(issuer)
      .withSubject(subject)
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

  private val AUTH_KEY =
    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  private def runInterceptor(
      interceptor: JwtAuthInterceptor,
      headerValue: Option[String]
  ): (ServerCall[String, String], ServerCallHandler[String, String]) = {
    val call    = mock(classOf[ServerCall[String, String]])
    val handler = mock(classOf[ServerCallHandler[String, String]])
    val headers = new Metadata()
    headerValue.foreach(v => headers.put(AUTH_KEY, v))
    interceptor.interceptCall(call, headers, handler)
    (call, handler)
  }

  test("rejects with UNAUTHENTICATED when Authorization header is missing") {
    val interceptor     = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) = runInterceptor(interceptor, None)

    val statusCap = ArgumentCaptor.forClass(classOf[Status])
    verify(call).close(statusCap.capture(), any(classOf[Metadata]))
    statusCap.getValue.getCode shouldBe Status.UNAUTHENTICATED.getCode
    verify(handler, never()).startCall(any(), any())
  }

  test("allows the call when the JWT subject matches the configured owner") {
    val interceptor = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) =
      runInterceptor(interceptor, Some("Bearer " + tokenFor("alice@example.com")))

    verify(call, never()).close(any(), any())
    verify(handler).startCall(any(), any())
  }

  test("trims surrounding whitespace from the configured owner") {
    val interceptor = new JwtAuthInterceptor(validatorFor(), "  alice@example.com  ")
    val (call, handler) =
      runInterceptor(interceptor, Some("Bearer " + tokenFor("alice@example.com")))

    verify(call, never()).close(any(), any())
    verify(handler).startCall(any(), any())
  }

  test("rejects with PERMISSION_DENIED when the JWT subject does not match the owner") {
    val interceptor = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) =
      runInterceptor(interceptor, Some("Bearer " + tokenFor("eve@example.com")))

    val statusCap = ArgumentCaptor.forClass(classOf[Status])
    verify(call).close(statusCap.capture(), any(classOf[Metadata]))
    statusCap.getValue.getCode shouldBe Status.PERMISSION_DENIED.getCode
    verify(handler, never()).startCall(any(), any())
  }

  test("rejects with UNAUTHENTICATED when Authorization header is not a bearer token") {
    val interceptor     = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) = runInterceptor(interceptor, Some("Basic dXNlcjpwYXNz"))

    val statusCap = ArgumentCaptor.forClass(classOf[Status])
    verify(call).close(statusCap.capture(), any(classOf[Metadata]))
    statusCap.getValue.getCode shouldBe Status.UNAUTHENTICATED.getCode
    verify(handler, never()).startCall(any(), any())
  }

  test("rejects with UNAUTHENTICATED when the bearer token is empty") {
    val interceptor     = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) = runInterceptor(interceptor, Some("Bearer   "))

    val statusCap = ArgumentCaptor.forClass(classOf[Status])
    verify(call).close(statusCap.capture(), any(classOf[Metadata]))
    statusCap.getValue.getCode shouldBe Status.UNAUTHENTICATED.getCode
    verify(handler, never()).startCall(any(), any())
  }

  test("rejects with UNAUTHENTICATED when JWT signature is invalid") {
    val wrongKeyPair = {
      val gen = KeyPairGenerator.getInstance("RSA"); gen.initialize(2048); gen.generateKeyPair()
    }
    val wrongAlg = Algorithm.RSA256(
      wrongKeyPair.getPublic.asInstanceOf[RSAPublicKey],
      wrongKeyPair.getPrivate.asInstanceOf[RSAPrivateKey]
    )
    val tamperedToken = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(wrongAlg)

    val interceptor     = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) = runInterceptor(interceptor, Some("Bearer " + tamperedToken))

    val statusCap = ArgumentCaptor.forClass(classOf[Status])
    verify(call).close(statusCap.capture(), any(classOf[Metadata]))
    statusCap.getValue.getCode shouldBe Status.UNAUTHENTICATED.getCode
    verify(handler, never()).startCall(any(), any())
  }

  test("accepts the bearer prefix case-insensitively") {
    val interceptor = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) =
      runInterceptor(interceptor, Some("bearer " + tokenFor("alice@example.com")))

    verify(call, never()).close(any(), any())
    verify(handler).startCall(any(), any())
  }

  test("rejects with UNAUTHENTICATED when the token has no subject") {
    val tokenWithoutSubject = JWT
      .create()
      .withIssuer(issuer)
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val interceptor     = new JwtAuthInterceptor(validatorFor(), "alice@example.com")
    val (call, handler) = runInterceptor(interceptor, Some("Bearer " + tokenWithoutSubject))

    val statusCap = ArgumentCaptor.forClass(classOf[Status])
    verify(call).close(statusCap.capture(), any(classOf[Metadata]))
    statusCap.getValue.getCode shouldBe Status.UNAUTHENTICATED.getCode
    verify(handler, never()).startCall(any(), any())
  }

  test("throws when owner is null") {
    an[IllegalStateException] should be thrownBy new JwtAuthInterceptor(validatorFor(), null)
  }

  test("throws when owner is blank") {
    an[IllegalStateException] should be thrownBy new JwtAuthInterceptor(validatorFor(), "  ")
  }

  test("exposes a public no-arg constructor for Spark Connect's interceptor registry") {
    // Spark Connect instantiates spark.connect.grpc.interceptor.classes by finding a
    // zero-arg constructor; a SparkConf-only constructor is not used. Guard that contract.
    val ctor = classOf[JwtAuthInterceptor].getConstructors.find(_.getParameterCount == 0)
    ctor.isDefined shouldBe true
    java.lang.reflect.Modifier.isPublic(ctor.get.getModifiers) shouldBe true
  }

  test("SparkConf constructor throws when owner is missing") {
    val conf = new SparkConf(false)
      .set("spark.armada.connect.oidc.issuerUrl", issuer)
      .set("spark.armada.connect.oidc.jwksUrl", "https://idp.test/jwks")
    an[IllegalStateException] should be thrownBy new JwtAuthInterceptor(conf)
  }

  test("SparkConf constructor builds when owner and issuer are set") {
    val conf = new SparkConf(false)
      .set("spark.armada.connect.owner", "alice@example.com")
      .set("spark.armada.connect.oidc.issuerUrl", issuer)
      .set("spark.armada.connect.oidc.jwksUrl", "https://idp.test/jwks")
    noException should be thrownBy new JwtAuthInterceptor(conf)
  }
}
