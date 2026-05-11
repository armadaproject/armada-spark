# Components

Code-level walkthrough of the OAuth implementation.

## `OAuthSidecarBuilder`

File: `[src/main/scala/org/apache/spark/deploy/armada/submit/OAuthSidecarBuilder.scala](../../src/main/scala/org/apache/spark/deploy/armada/submit/OAuthSidecarBuilder.scala)` (358 lines).

A `private[spark] object` with two public methods.

### `buildOAuthSidecar(conf: SparkConf): Option[Container]`

Returns `None` when `spark.armada.oauth.enabled=false` so callers can write:

```scala
val initContainers = existingInitContainers ++ OAuthSidecarBuilder.buildOAuthSidecar(conf).toSeq
```

When enabled, returns a fully-constructed Kubernetes `Container` ready to be appended to `initContainers`.

### `getOAuthProxyPort(conf: SparkConf): Option[Int]`

```scala
def getOAuthProxyPort(conf: SparkConf): Option[Int] =
  if (conf.get(Config.ARMADA_OAUTH_ENABLED)) Some(conf.get(Config.ARMADA_OAUTH_PROXY_PORT))
  else None
```

Used by the networking integration to determine the effective UI port.

### `buildOAuthContainer` (lines 55-88)

```scala
generated.Container(
  name          = Some("oauth"),
  image         = Some(proxyImage),
  args          = args,
  env           = envVars,
  volumeMounts  = volumeMounts,
  ports         = Seq(ContainerPort(
                    containerPort = Some(proxyPort),
                    name = Some("oauth"),
                    protocol = Some("TCP"))),
  resources     = Some(resources),
  restartPolicy = Some("Always"),
  livenessProbe = Some(livenessProbe)
)
```

`restartPolicy = "Always"` is what makes this a native sidecar in K8s 1.28+. The single port is named `oauth` so Armada can route by name.

### `validateOAuthConfig` (lines 91-126)

Three preconditions enforced via `require(...)`:

1. `clientId` must be defined.
2. Either `clientSecret` (inline) or `clientSecretK8s` must be set.
3. If `skipProviderDiscovery` is off, `issuerUrl` is required. If on, all four explicit endpoints are required (`loginUrl`, `redeemUrl`, `validateUrl`, `jwksUrl`).

Failures throw `IllegalArgumentException` at submission time.

### `buildOAuthProxyArgs` (lines 129-221)

Translates Spark config to oauth2-proxy CLI argv. Composition:

```
coreArgs ++ oidcArgs ++ securityArgs ++ cookieArgs ++ extraAudiencesArgs
```

**Core (always present):** `--pass-authorization-header`, `--http-address 0.0.0.0:<proxyPort>`, `--reverse-proxy`, `--provider oidc`, `--provider-display-name`, `--client-id`, `--email-domain`, `--upstream http://127.0.0.1:<sparkUIPort>`, `--code-challenge-method`.

**OIDC URLs (per-key, only if set):**


| Spark key                        | oauth2-proxy flag   |
| -------------------------------- | ------------------- |
| `spark.armada.oauth.redirectUrl` | `--redirect-url`    |
| `spark.armada.oauth.issuerUrl`   | `--oidc-issuer-url` |
| `spark.armada.oauth.loginUrl`    | `--login-url`       |
| `spark.armada.oauth.redeemUrl`   | `--redeem-url`      |
| `spark.armada.oauth.validateUrl` | `--validate-url`    |
| `spark.armada.oauth.jwksUrl`     | `--oidc-jwks-url`   |


**Security/cookie booleans:** `--cookie-secure`, `--pass-host-header`, `--skip-oidc-discovery`, `--insecure-oidc-skip-issuer-verification`, `--insecure-oidc-allow-unverified-email`, `--skip-provider-button`, `--skip-auth-preflight`, `--cookie-csrf-per-request`, plus the bare flags `--skip-jwt-bearer-tokens`, `--ssl-insecure-skip-verify`, `--ssl-upstream-insecure-skip-verify`.

**Cookie:** `--cookie-name`, `--cookie-path`, optional `--cookie-samesite`, `--cookie-csrf-expire`, `--whitelist-domain`.

**Extra audiences:** `extraAudiences` is split on commas; each token becomes a separate `--oidc-extra-audience <value>`.

### `buildOAuthEnvVars` (lines 254-290)

Two env vars, always:

- `OAUTH2_PROXY_COOKIE_SECRET`: 16 random bytes from `SecureRandom`, hex-encoded. Regenerated per submission, so sessions don't survive a driver restart.
- `OAUTH2_PROXY_CLIENT_SECRET`: either inlined from `clientSecret` (visible via `kubectl describe`, dev only) or sourced from a `valueFrom.secretKeyRef` to `clientSecretK8s` (recommended).

### `buildOAuthVolumeMounts` (lines 293-312)

Optional mounts for `caCertPath` / `caBundlePath`. Mounted `readOnly: true`. The volumes themselves must already exist on the pod (typically supplied via a job template); the builder only adds the mounts.

### `buildLivenessProbe` (lines 331-350)

`HTTP GET /ping` on the proxy port. `initialDelaySeconds=5`, `periodSeconds=10`, `timeoutSeconds=3`, `failureThreshold=3`. Kubernetes restarts the container after three consecutive failures.

### `buildOAuthResources` (lines 315-321)

Same map for `requests` and `limits` (Armada requirement). Defaults: `100m` CPU, `128Mi` memory.

## Integration points in `ArmadaClientApplication`

File: `[src/main/scala/org/apache/spark/deploy/armada/submit/ArmadaClientApplication.scala](../../src/main/scala/org/apache/spark/deploy/armada/submit/ArmadaClientApplication.scala)`. Four touchpoints; see [Networking](networking.md) for port routing details.

### `getEffectiveUIPort` (lines 162-167)

Single source of truth for "what port faces outside?". Both the Service builder and the Ingress builder use this.

### Sidecar attachment (line 999)

```scala
val oauthSidecar      = OAuthSidecarBuilder.buildOAuthSidecar(conf)
val allInitContainers = currentPodSpec.initContainers ++ oauthSidecar.toSeq
```

Appended after user-supplied init containers, so a job template cannot override it.

### `buildDriverContainerPorts` (lines 1456-1480)

```scala
val needsUIPort = ingressEnabled && !oauthEnabled
```

When OAuth is on, the Spark UI port is not added to the driver container's `ports` list.

### `buildServiceConfig` and `resolveIngressConfig` (lines 1638-1678)

Both use `getEffectiveUIPort`. Ingress precedence: CLI > Job Template > Default. Annotations merged with CLI winning.

## Tests

File: `[OAuthSidecarBuilderSuite.scala](../../src/test/scala/org/apache/spark/deploy/armada/submit/OAuthSidecarBuilderSuite.scala)` (8 tests).

Coverage is limited to container-shape and validation. **Coverage gap**: no end-to-end test runs against a real OIDC provider; that requires standing up Dex/Keycloak in the kind cluster and submitting a long-running job manually.