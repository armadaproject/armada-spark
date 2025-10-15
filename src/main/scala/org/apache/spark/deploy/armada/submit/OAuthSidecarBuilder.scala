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
import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config

import java.security.SecureRandom
import java.util.Base64

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

  /** Builds OAuth2-proxy sidecar container if OAuth is enabled.
    *
    * @param conf
    *   SparkConf containing OAuth configuration
    * @return
    *   Some(Container) if OAuth enabled, None otherwise
    */
  def buildOAuthSidecar(conf: SparkConf): Option[generated.Container] = {
    if (!conf.get(Config.ARMADA_OAUTH_ENABLED)) {
      return None
    }

    validateOAuthConfig(conf)

    val clientId     = conf.get(Config.ARMADA_OAUTH_CLIENT_ID).get
    val proxyImage   = conf.get(Config.ARMADA_OAUTH_PROXY_IMAGE)
    val proxyPort    = conf.get(Config.ARMADA_OAUTH_PROXY_PORT)
    val providerName = conf.get(Config.ARMADA_OAUTH_PROVIDER_DISPLAY_NAME)
    val sparkUIPort  = conf.getInt("spark.ui.port", 4040)

    val args         = buildOAuthProxyArgs(conf, clientId, proxyPort, sparkUIPort, providerName)
    val envVars      = buildOAuthEnvVars(conf)
    val volumeMounts = buildOAuthVolumeMounts(conf)
    val resources    = buildOAuthResources(conf)

    val oauthContainer = generated.Container(
      name = Some("oauth"),
      image = Some(proxyImage),
      args = args,
      env = envVars,
      volumeMounts = if (volumeMounts.nonEmpty) volumeMounts else Seq.empty,
      // Native sidecars (restartPolicy: Always) have ports extracted by Armada for service creation
      ports = Seq(
        generated.ContainerPort(
          containerPort = Some(proxyPort),
          name = Some("oauth"),
          protocol = Some("TCP")
        )
      ),
      resources = Some(resources),
      restartPolicy = Some("Always")
    )

    Some(oauthContainer)
  }

  /** Validates required OAuth configuration.
    *
    * @param conf
    *   SparkConf to validate
    * @throws IllegalArgumentException
    *   if required configs are missing
    */
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

    val skipDiscovery = conf.get(Config.ARMADA_OAUTH_SKIP_PROVIDER_DISCOVERY)
    if (skipDiscovery) {
      // When skipping discovery, explicit endpoints are required
      require(
        conf.get(Config.ARMADA_OAUTH_LOGIN_URL).isDefined,
        s"OAuth skip-oidc-discovery enabled but ${Config.ARMADA_OAUTH_LOGIN_URL.key} is not set"
      )
      require(
        conf.get(Config.ARMADA_OAUTH_REDEEM_URL).isDefined,
        s"OAuth skip-oidc-discovery enabled but ${Config.ARMADA_OAUTH_REDEEM_URL.key} is not set"
      )
      require(
        conf.get(Config.ARMADA_OAUTH_VALIDATE_URL).isDefined,
        s"OAuth skip-oidc-discovery enabled but ${Config.ARMADA_OAUTH_VALIDATE_URL.key} is not set"
      )
      require(
        conf.get(Config.ARMADA_OAUTH_JWKS_URL).isDefined,
        s"OAuth skip-oidc-discovery enabled but ${Config.ARMADA_OAUTH_JWKS_URL.key} is not set"
      )
    } else {
      // When using discovery, issuer URL is required
      require(
        conf.get(Config.ARMADA_OAUTH_ISSUER_URL).isDefined,
        s"OAuth enabled but ${Config.ARMADA_OAUTH_ISSUER_URL.key} is not set (required unless skipProviderDiscovery=true)"
      )
    }
  }

  /** Builds OAuth2-proxy command-line arguments.
    *
    * @return
    *   Sequence of arguments for oauth2-proxy
    */
  private def buildOAuthProxyArgs(
      conf: SparkConf,
      clientId: String,
      proxyPort: Int,
      sparkUIPort: Int,
      providerName: String
  ): Seq[String] = {
    val baseArgs = Seq(
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

    // Optional redirect-url and oidc-issuer-url
    val optionalBaseArgs = Seq(
      conf.get(Config.ARMADA_OAUTH_REDIRECT_URL).toSeq.flatMap(url => Seq("--redirect-url", url)),
      conf.get(Config.ARMADA_OAUTH_ISSUER_URL).toSeq.flatMap(url => Seq("--oidc-issuer-url", url))
    ).flatten

    // Boolean flags that take "true" as a value
    val booleanWithValueArgs = Seq(
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
      (Config.ARMADA_OAUTH_PASS_HOST_HEADER, "--pass-host-header"),
      (Config.ARMADA_OAUTH_COOKIE_SECURE, "--cookie-secure"),
      (Config.ARMADA_OAUTH_COOKIE_CSRF_PER_REQUEST, "--cookie-csrf-per-request")
    ).flatMap { case (configEntry, argName) =>
      if (conf.get(configEntry)) Seq(argName, "true")
      else Seq.empty
    }

    // Boolean flags without values
    val simpleBooleanArgs = Seq(
      (Config.ARMADA_OAUTH_SKIP_JWT_BEARER_TOKENS, "--skip-jwt-bearer-tokens"),
      (Config.ARMADA_OAUTH_SKIP_VERIFY, "--ssl-insecure-skip-verify")
    ).flatMap { case (configEntry, argName) =>
      if (conf.get(configEntry)) Seq(argName)
      else Seq.empty
    }

    // Optional string value args
    val optionalStringValueArgs = Seq(
      (Config.ARMADA_OAUTH_LOGIN_URL, "--login-url"),
      (Config.ARMADA_OAUTH_REDEEM_URL, "--redeem-url"),
      (Config.ARMADA_OAUTH_VALIDATE_URL, "--validate-url"),
      (Config.ARMADA_OAUTH_JWKS_URL, "--oidc-jwks-url"),
      (Config.ARMADA_OAUTH_COOKIE_SAMESITE, "--cookie-samesite"),
      (Config.ARMADA_OAUTH_COOKIE_CSRF_EXPIRE, "--cookie-csrf-expire"),
      (Config.ARMADA_OAUTH_WHITELIST_DOMAIN, "--whitelist-domain")
    ).flatMap { case (configEntry, argName) =>
      conf.get(configEntry).toSeq.flatMap(value => Seq(argName, value))
    }

    // Non-optional string value args (have defaults)
    val nonOptionalStringValueArgs = Seq(
      Seq("--cookie-name", conf.get(Config.ARMADA_OAUTH_COOKIE_NAME)),
      Seq("--cookie-path", conf.get(Config.ARMADA_OAUTH_COOKIE_PATH))
    ).flatten

    val extraAudiencesArgs =
      conf.get(Config.ARMADA_OAUTH_EXTRA_AUDIENCES).toSeq.flatMap { audiences =>
        audiences.split(",").flatMap { audience =>
          Seq("--oidc-extra-audience", audience.trim)
        }
      }

    baseArgs ++ optionalBaseArgs ++ booleanWithValueArgs ++ simpleBooleanArgs ++
      optionalStringValueArgs ++ nonOptionalStringValueArgs ++ extraAudiencesArgs
  }

  /** Builds environment variables for OAuth2-proxy.
    *
    * Handles client secret from either inline config or Kubernetes secret.
    *
    * @param conf
    *   SparkConf containing OAuth configuration
    * @return
    *   Sequence of environment variables
    */
  private def buildOAuthEnvVars(conf: SparkConf): Seq[generated.EnvVar] = {
    val cookieSecretEnv = generated.EnvVar(
      name = Some("OAUTH2_PROXY_COOKIE_SECRET"),
      value = Some(generateCookieSecret())
    )

    val clientSecretEnv = conf.get(Config.ARMADA_OAUTH_CLIENT_SECRET) match {
      case Some(secret) =>
        // Inline secret (simple but less secure)
        generated.EnvVar(
          name = Some("OAUTH2_PROXY_CLIENT_SECRET"),
          value = Some(secret)
        )
      case None =>
        // Kubernetes secret reference (secure)
        val secretName = conf.get(Config.ARMADA_OAUTH_CLIENT_SECRET_K8S).get
        generated.EnvVar(
          name = Some("OAUTH2_PROXY_CLIENT_SECRET"),
          valueFrom = Some(
            generated.EnvVarSource(
              secretKeyRef = Some(
                generated.SecretKeySelector(
                  localObjectReference = Some(
                    generated.LocalObjectReference(name = Some(secretName))
                  ),
                  key = Some("client-secret")
                )
              )
            )
          )
        )
    }

    Seq(cookieSecretEnv, clientSecretEnv)
  }

  /** Builds optional volume mounts for TLS certificates.
    *
    * Only creates volume mounts if paths are explicitly configured. No defaults are provided.
    *
    * @param conf
    *   SparkConf containing optional TLS certificate paths
    * @return
    *   Sequence of volume mounts (empty if not configured)
    */
  private def buildOAuthVolumeMounts(conf: SparkConf): Seq[generated.VolumeMount] = {
    val caCertMount = conf.get(Config.ARMADA_OAUTH_TLS_CA_CERT_PATH).map { path =>
      generated.VolumeMount(
        name = Some("ca-certificates"),
        mountPath = Some(path),
        readOnly = Some(true)
      )
    }

    val caBundleMount = conf.get(Config.ARMADA_OAUTH_TLS_CA_BUNDLE_PATH).map { path =>
      generated.VolumeMount(
        name = Some("ca-bundle"),
        mountPath = Some(path),
        readOnly = Some(true)
      )
    }

    Seq(caCertMount, caBundleMount).flatten
  }

  /** Builds resource requirements for OAuth2-proxy container.
    *
    * Armada requires all containers to have resources specified, and requests must equal limits.
    *
    * @param conf
    *   SparkConf containing resource configuration
    * @return
    *   ResourceRequirements with requests == limits
    */
  private def buildOAuthResources(conf: SparkConf): generated.ResourceRequirements = {
    val cpu    = conf.get(Config.ARMADA_OAUTH_RESOURCES_CPU)
    val memory = conf.get(Config.ARMADA_OAUTH_RESOURCES_MEMORY)

    val resourceMap = Map(
      "cpu"    -> Quantity(string = Some(cpu)),
      "memory" -> Quantity(string = Some(memory))
    )

    generated.ResourceRequirements(
      limits = resourceMap,
      requests = resourceMap // Armada requires requests == limits
    )
  }

  /** Generates a random cookie secret for OAuth2-proxy session encryption.
    *
    * Uses SecureRandom to generate 16 cryptographically secure random bytes, then hex encodes to 32
    * chars. OAuth2-proxy requires cookie_secret to be 16, 24, or 32 bytes (not base64 encoded).
    *
    * @return
    *   32-character hex-encoded cookie secret (16 bytes)
    */
  private def generateCookieSecret(): String = {
    val random = new SecureRandom()
    val bytes  = new Array[Byte](16) // 16 bytes = 32 hex chars = 32 bytes as string
    random.nextBytes(bytes)
    bytes.map("%02x".format(_)).mkString
  }

  /** Returns the OAuth proxy port if OAuth is enabled, otherwise None.
    *
    * Used for Service port selection - when OAuth is enabled, Service should route to OAuth proxy
    * port instead of Spark UI port.
    *
    * @param conf
    *   SparkConf containing OAuth configuration
    * @return
    *   Some(proxyPort) if OAuth enabled, None otherwise
    */
  def getOAuthProxyPort(conf: SparkConf): Option[Int] = {
    if (conf.get(Config.ARMADA_OAUTH_ENABLED)) {
      Some(conf.get(Config.ARMADA_OAUTH_PROXY_PORT))
    } else {
      None
    }
  }
}
