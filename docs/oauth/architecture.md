# Architecture

## System view

The OAuth integration adds a single sidecar container to the Spark driver pod and re-routes the Ingress so that all UI traffic passes through it.

```mermaid
flowchart TB
    User([User browser])

    subgraph k8s["Kubernetes Cluster"]
        Ingress["Ingress<br/><i>TLS terminates here</i>"]

        subgraph pod["Driver Pod"]
            Proxy["oauth2-proxy<br/>native sidecar · :4180"]
            Driver["Spark Driver<br/>UI on 127.0.0.1:4040"]
        end

        Ingress -->|routes to :4180| Proxy
        Proxy -->|loopback| Driver
    end

    OIDC[(OIDC Provider)]

    User ==>|HTTPS| Ingress
    User -.->|login redirects| OIDC
    Proxy -.->|token exchange · JWKS| OIDC
```

The browser only ever sees the Ingress. oauth2-proxy is the only component that talks to the Spark UI directly, over loopback inside the same pod. The OIDC provider is reached by both the browser (during the redirect dance) and the proxy (for token exchange and JWKS lookups).

## Component responsibilities

### oauth2-proxy sidecar

A long-running init container with `restartPolicy: Always` (Kubernetes 1.28+ "native sidecar" pattern). It:

- Serves browser HTTP on `0.0.0.0:4180` (configurable via `spark.armada.oauth.proxy.port`).
- Drives the OIDC redirect dance (`/oauth2/start`, `/oauth2/callback`).
- Verifies the ID token signature against the provider's JWKS and checks `iss` / `aud` / `exp` / `email_verified`.
- Encrypts session state into a cookie and validates it on every subsequent request.
- Reverse-proxies authenticated requests to `http://127.0.0.1:<sparkUIPort>`.

### Spark driver container

Runs the Spark application. The Spark UI is part of the driver process (Jetty servlet on `spark.ui.port`, default 4040). When OAuth is enabled, the UI port is not added to the driver container's `ports` list; the proxy reaches it on `127.0.0.1`. Spark UI is fully OAuth-unaware.

### Headless Service

Created by Armada from the pod spec, exposing `driverPort` (typically 7078, used by executors via `service-0`) and the effective UI port (4180 with OAuth on, 4040 otherwise).

### Ingress

Created by Armada from the `IngressConfig` armada-spark builds in [`resolveIngressConfig`](../../src/main/scala/org/apache/spark/deploy/armada/submit/ArmadaClientApplication.scala). Backend port is the effective UI port. TLS, certificate, and annotations follow precedence **CLI > Job Template > Default**.

### OIDC provider (external)

Any OIDC-compliant provider. Either supply `issuerUrl` for discovery, or set explicit endpoints via `skipProviderDiscovery=true`.

## Deployment view (pod-level)

```mermaid
flowchart TB
    subgraph pod[Driver Pod]
        direction TB

        subgraph initContainers[initContainers]
            oauth["oauth (always restarts)<br/>image: oauth2-proxy:v7.5.1<br/>port: 4180<br/>upstream: 127.0.0.1:4040"]
            templates["user-supplied init containers<br/>(if any, from job template)"]
        end

        subgraph containers[containers]
            driver["spark-driver<br/>image: spark<br/>port: 7078 driver RPC<br/>port: 4040 UI bound to all but only used via loopback"]
            usersidecars["user-supplied sidecars<br/>(if any, from job template)"]
        end

        subgraph volumes[Volumes]
            tlsvol1["ca-certificates<br/>(optional)"]
            tlsvol2["ca-bundle<br/>(optional)"]
        end
    end

    oauth -.->|reads from secret| secrets[(K8s Secret<br/>client-secret)]
```

The OAuth sidecar is in `initContainers` with `restartPolicy: Always`, so it starts before the driver and runs alongside it. TLS volumes are added only when `caCertPath` or `caBundlePath` is configured. The K8s secret is referenced only when `clientSecretK8s` is set; otherwise the secret value is inlined.

## Lifecycle

```mermaid
stateDiagram-v2
    [*] --> PodScheduled: spark-submit
    PodScheduled --> InitContainersStarting: pod admitted
    InitContainersStarting --> ProxyReady: oauth2-proxy ping OK
    ProxyReady --> DriverStarting: native sidecar continues
    DriverStarting --> DriverRunning: Spark UI up on port 4040
    DriverRunning --> AcceptingTraffic: ingress healthy
    AcceptingTraffic --> DriverComplete: job finishes
    DriverComplete --> ProxyShutdown: pod terminating
    ProxyShutdown --> [*]: pod deleted
```

The proxy starts before the driver and terminates with the pod. There is no surface that outlives the driver.

## Constraints

- **Cluster-mode submission only.** OAuth applies to the driver pod, which only exists in cluster-mode.
- **Kubernetes 1.28+** for native sidecar semantics. Older versions treat `restartPolicy: Always` on an init container as a one-shot.
- **Ingress controller required** in the cluster.
- **OIDC provider reachable** from both the cluster (token exchange, JWKS) and the user's browser (authorize redirect).
