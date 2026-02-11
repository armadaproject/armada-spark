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

import k8s.io.api.core.v1.generated
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import k8s.io.apimachinery.pkg.util.intstr.generated.IntOrString
import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config
import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}

import java.security.SecureRandom

/** Builds OAuth2-proxy sidecar container for Spark driver WebUI authentication.
  *
  * Provides OAuth integration - users configure Spark properties and the OAuth proxy runs as a
  * native sidecar (initContainer with restartPolicy=Always), terminating with the driver.
  *
  * Configuration: See Config.scala for all spark.armada.oauth.* properties and defaults.
  * Documentation: See docs/ui.md for setup examples and docs/architecture.md for parameter
  * reference.
  */
private[spark] object OAuthSidecarBuilder {

  // Volume names for TLS certificate mounts
  private val CA_CERTIFICATES_VOLUME = "ca-certificates"
  private val CA_BUNDLE_VOLUME       = "ca-bundle"

  // Liveness probe configuration
  private val LIVENESS_INITIAL_DELAY_SECONDS = 5
  private val LIVENESS_PERIOD_SECONDS        = 10
  private val LIVENESS_TIMEOUT_SECONDS       = 3
  private val LIVENESS_FAILURE_THRESHOLD     = 3

  /** Builds OAuth2-proxy sidecar container if OAuth is enabled. */
  def buildOAuthSidecar(conf: SparkConf): Option[generated.Container] = {
    if (conf.get(Config.ARMADA_OAUTH_ENABLED)) Some(buildOAuthContainer(conf)) else None
  }

  private def buildOAuthContainer(conf: SparkConf): generated.Container = {
    validateOAuthConfig(conf)

    val clientId     = conf.get(Config.ARMADA_OAUTH_CLIENT_ID).get
    val proxyImage   = conf.get(Config.ARMADA_OAUTH_PROXY_IMAGE)
    val proxyPort    = conf.get(Config.ARMADA_OAUTH_PROXY_PORT)
    val providerName = conf.get(Config.ARMADA_OAUTH_PROVIDER_DISPLAY_NAME)
    val sparkUIPort  = conf.getInt("spark.ui.port", 4040)

    val args          = buildOAuthProxyArgs(conf, clientId, proxyPort, sparkUIPort, providerName)
    val envVars       = buildOAuthEnvVars(conf)
    val volumeMounts  = buildOAuthVolumeMounts(conf)
    val resources     = buildOAuthResources(conf)
    val livenessProbe = buildLivenessProbe(proxyPort)

    generated.Container(
      name = Some("oauth"),
      image = Some(proxyImage),
      args = args,
      env = envVars,
      volumeMounts = volumeMounts,
      // Native sidecars (restartPolicy: Always) have ports extracted by Armada for service creation
      ports = Seq(
        generated.ContainerPort(
          containerPort = Some(proxyPort),
          name = Some("oauth"),
          protocol = Some("TCP")
        )
      ),
      resources = Some(resources),
      restartPolicy = Some("Always"),
      livenessProbe = Some(livenessProbe)
    )
  }

  /** Validates required OAuth configuration. Assumes ARMADA_OAUTH_ENABLED is true. */
  private def validateOAuthConfig(conf: SparkConf): Unit = {
    require(
      conf.get(Config.ARMADA_OAUTH_CLIENT_ID).isDefined,
      s"OAuth enabled but ${Config.ARMADA_OAUTH_CLIENT_ID.key} is not set"
    )
    require(
      conf.get(Config.ARMADA_OAUTH_CLIENT_SECRET).isDefined ||
        conf.get(Config.ARMADA_OAUTH_CLIENT_SECRET_K8S).isDefined,
      s"OAuth enabled but neither ${Config.ARMADA_OAUTH_CLIENT_SECRET.key} nor ${Config.ARMADA_OAUTH_CLIENT_SECRET_K8S.key} is set"
    )

    if (conf.get(Config.ARMADA_OAUTH_SKIP_PROVIDER_DISCOVERY)) {
      validateExplicitEndpoints(conf)
    } else {
      require(
        conf.get(Config.ARMADA_OAUTH_ISSUER_URL).isDefined,
        s"OAuth enabled but ${Config.ARMADA_OAUTH_ISSUER_URL.key} is not set (required when skipProviderDiscovery is false)"
      )
    }
  }

  /** Validates explicit OIDC endpoints when discovery is disabled. */
  private def validateExplicitEndpoints(conf: SparkConf): Unit = {
    val requiredEndpoints = Seq(
      (Config.ARMADA_OAUTH_LOGIN_URL, "loginUrl"),
      (Config.ARMADA_OAUTH_REDEEM_URL, "redeemUrl"),
      (Config.ARMADA_OAUTH_VALIDATE_URL, "validateUrl"),
      (Config.ARMADA_OAUTH_JWKS_URL, "jwksUrl")
    )
    requiredEndpoints.foreach { case (configEntry, name) =>
      require(
        conf.get(configEntry).isDefined,
        s"OAuth skip-oidc-discovery enabled but ${configEntry.key} is not set"
      )
    }
  }

  /** Builds OAuth2-proxy command-line arguments organized by category. */
  private def buildOAuthProxyArgs(
      conf: SparkConf,
      clientId: String,
      proxyPort: Int,
      sparkUIPort: Int,
      providerName: String
  ): Seq[String] = {
    val coreArgs = Seq(
      "--pass-authorization-header",
      "--http-address",
      s"0.0.0.0:$proxyPort",
      "--reverse-proxy",
      "--provider",
      "oidc",
      "--provider-display-name",
      providerName,
      "--client-id",
      clientId,
      "--email-domain",
      conf.get(Config.ARMADA_OAUTH_EMAIL_DOMAIN),
      "--upstream",
      s"http://127.0.0.1:$sparkUIPort",
      "--code-challenge-method",
      conf.get(Config.ARMADA_OAUTH_CODE_CHALLENGE_METHOD)
    )

    val oidcArgs = buildOptionalArgs(
      conf,
      Seq(
        (Config.ARMADA_OAUTH_REDIRECT_URL, "--redirect-url"),
        (Config.ARMADA_OAUTH_ISSUER_URL, "--oidc-issuer-url"),
        (Config.ARMADA_OAUTH_LOGIN_URL, "--login-url"),
        (Config.ARMADA_OAUTH_REDEEM_URL, "--redeem-url"),
        (Config.ARMADA_OAUTH_VALIDATE_URL, "--validate-url"),
        (Config.ARMADA_OAUTH_JWKS_URL, "--oidc-jwks-url")
      )
    )

    val securityArgs = buildBooleanArgsWithValue(
      conf,
      Seq(
        (Config.ARMADA_OAUTH_SKIP_PROVIDER_DISCOVERY, "--skip-oidc-discovery"),
        (
          Config.ARMADA_OAUTH_INSECURE_SKIP_ISSUER_VERIFICATION,
          "--insecure-oidc-skip-issuer-verification"
        ),
        (
          Config.ARMADA_OAUTH_INSECURE_ALLOW_UNVERIFIED_EMAIL,
          "--insecure-oidc-allow-unverified-email"
        ),
        (Config.ARMADA_OAUTH_SKIP_PROVIDER_BUTTON, "--skip-provider-button"),
        (Config.ARMADA_OAUTH_SKIP_AUTH_PREFLIGHT, "--skip-auth-preflight"),
        (Config.ARMADA_OAUTH_PASS_HOST_HEADER, "--pass-host-header")
      )
    ) ++ buildBooleanFlags(
      conf,
      Seq(
        (Config.ARMADA_OAUTH_SKIP_JWT_BEARER_TOKENS, "--skip-jwt-bearer-tokens"),
        (Config.ARMADA_OAUTH_SKIP_VERIFY, "--ssl-insecure-skip-verify"),
        (
          Config.ARMADA_OAUTH_SSL_UPSTREAM_INSECURE_SKIP_VERIFY,
          "--ssl-upstream-insecure-skip-verify"
        )
      )
    )

    val cookieArgs = Seq(
      "--cookie-name",
      conf.get(Config.ARMADA_OAUTH_COOKIE_NAME),
      "--cookie-path",
      conf.get(Config.ARMADA_OAUTH_COOKIE_PATH)
    ) ++ buildBooleanArgsWithValue(
      conf,
      Seq(
        (Config.ARMADA_OAUTH_COOKIE_SECURE, "--cookie-secure"),
        (Config.ARMADA_OAUTH_COOKIE_CSRF_PER_REQUEST, "--cookie-csrf-per-request")
      )
    ) ++ buildOptionalArgs(
      conf,
      Seq(
        (Config.ARMADA_OAUTH_COOKIE_SAMESITE, "--cookie-samesite"),
        (Config.ARMADA_OAUTH_COOKIE_CSRF_EXPIRE, "--cookie-csrf-expire"),
        (Config.ARMADA_OAUTH_WHITELIST_DOMAIN, "--whitelist-domain")
      )
    )

    val extraAudiencesArgs =
      conf.get(Config.ARMADA_OAUTH_EXTRA_AUDIENCES).toSeq.flatMap { audiences =>
        audiences.split(",").map(_.trim).flatMap(audience => Seq("--oidc-extra-audience", audience))
      }

    coreArgs ++ oidcArgs ++ securityArgs ++ cookieArgs ++ extraAudiencesArgs
  }

  /** Builds arguments for optional string config entries. */
  private def buildOptionalArgs(
      conf: SparkConf,
      configs: Seq[(OptionalConfigEntry[String], String)]
  ): Seq[String] = {
    configs.flatMap { case (configEntry, argName) =>
      conf.get(configEntry).toSeq.flatMap(value => Seq(argName, value))
    }
  }

  /** Builds boolean flags that require "true" as explicit value. */
  private def buildBooleanArgsWithValue(
      conf: SparkConf,
      configs: Seq[(ConfigEntry[Boolean], String)]
  ): Seq[String] = {
    configs.flatMap { case (configEntry, argName) =>
      if (conf.get(configEntry)) Seq(argName, "true") else Seq.empty
    }
  }

  /** Builds standalone boolean flags (no value). */
  private def buildBooleanFlags(
      conf: SparkConf,
      configs: Seq[(ConfigEntry[Boolean], String)]
  ): Seq[String] = {
    configs.flatMap { case (configEntry, argName) =>
      if (conf.get(configEntry)) Seq(argName) else Seq.empty
    }
  }

  /** Builds environment variables for OAuth2-proxy (cookie secret + client secret). */
  private def buildOAuthEnvVars(conf: SparkConf): Seq[generated.EnvVar] = {
    Seq(
      generated.EnvVar(
        name = Some("OAUTH2_PROXY_COOKIE_SECRET"),
        value = Some(generateCookieSecret())
      ),
      buildClientSecretEnvVar(conf)
    )
  }

  /** Builds the client secret environment variable from either inline config or K8s secret. */
  private def buildClientSecretEnvVar(conf: SparkConf): generated.EnvVar = {
    conf.get(Config.ARMADA_OAUTH_CLIENT_SECRET) match {
      case Some(secret) =>
        generated.EnvVar(
          name = Some("OAUTH2_PROXY_CLIENT_SECRET"),
          value = Some(secret)
        )
      case None =>
        val secretName = conf.get(Config.ARMADA_OAUTH_CLIENT_SECRET_K8S).get
        val secretKey  = conf.get(Config.ARMADA_OAUTH_CLIENT_SECRET_KEY)
        generated.EnvVar(
          name = Some("OAUTH2_PROXY_CLIENT_SECRET"),
          valueFrom = Some(
            generated.EnvVarSource(
              secretKeyRef = Some(
                generated.SecretKeySelector(
                  localObjectReference =
                    Some(generated.LocalObjectReference(name = Some(secretName))),
                  key = Some(secretKey)
                )
              )
            )
          )
        )
    }
  }

  /** Builds optional TLS certificate volume mounts if configured. */
  private def buildOAuthVolumeMounts(conf: SparkConf): Seq[generated.VolumeMount] = {
    Seq(
      conf.get(Config.ARMADA_OAUTH_TLS_CA_CERT_PATH).map { path =>
        generated.VolumeMount(
          name = Some(CA_CERTIFICATES_VOLUME),
          mountPath = Some(path),
          subPath = conf.get(Config.ARMADA_OAUTH_TLS_CA_CERT_SUBPATH),
          readOnly = Some(true)
        )
      },
      conf.get(Config.ARMADA_OAUTH_TLS_CA_BUNDLE_PATH).map { path =>
        generated.VolumeMount(
          name = Some(CA_BUNDLE_VOLUME),
          mountPath = Some(path),
          subPath = conf.get(Config.ARMADA_OAUTH_TLS_CA_BUNDLE_SUBPATH),
          readOnly = Some(true)
        )
      }
    ).flatten
  }

  /** Builds resource requirements (requests == limits, as required by Armada). */
  private def buildOAuthResources(conf: SparkConf): generated.ResourceRequirements = {
    val resourceMap = Map(
      "cpu"    -> Quantity(string = Some(conf.get(Config.ARMADA_OAUTH_RESOURCES_CPU))),
      "memory" -> Quantity(string = Some(conf.get(Config.ARMADA_OAUTH_RESOURCES_MEMORY)))
    )
    generated.ResourceRequirements(limits = resourceMap, requests = resourceMap)
  }

  /** Generates a 32-char hex-encoded cookie secret (16 random bytes). */
  private def generateCookieSecret(): String = {
    val bytes = new Array[Byte](16)
    new SecureRandom().nextBytes(bytes)
    bytes.map("%02x".format(_)).mkString
  }

  /** Builds liveness probe using oauth2-proxy's /ping endpoint. */
  private def buildLivenessProbe(proxyPort: Int): generated.Probe = {
    generated.Probe(
      handler = Some(
        generated.ProbeHandler(
          httpGet = Some(
            generated.HTTPGetAction(
              path = Some("/ping"),
              port = Some(IntOrString(intVal = Some(proxyPort))),
              scheme = Some("HTTP")
            )
          )
        )
      ),
      initialDelaySeconds = Some(LIVENESS_INITIAL_DELAY_SECONDS),
      periodSeconds = Some(LIVENESS_PERIOD_SECONDS),
      timeoutSeconds = Some(LIVENESS_TIMEOUT_SECONDS),
      failureThreshold = Some(LIVENESS_FAILURE_THRESHOLD),
      successThreshold = Some(1)
    )
  }

  /** Returns the OAuth proxy port if OAuth is enabled (for Service port selection). */
  def getOAuthProxyPort(conf: SparkConf): Option[Int] = {
    if (conf.get(Config.ARMADA_OAUTH_ENABLED)) Some(conf.get(Config.ARMADA_OAUTH_PROXY_PORT))
    else None
  }
}
