# Spark Driver UI Access

## Direct Access

### Port-forward (simplest)

```bash
kubectl -n <namespace> port-forward <driver-pod-name> 4040:4040
```

Then open: `http://localhost:4040`

**Finding the pod:** Check Lookout UI for job details. Pod name is typically `armada-<job-id>-0`.

### Basic Ingress (no auth)

```bash
--conf spark.armada.driver.ingress.enabled=true
```

**Warning:** Exposes UI publicly without authentication!

---

## OAuth2-Protected Access

Uses [oauth2-proxy](https://oauth2-proxy.github.io/oauth2-proxy/) as a native sidecar for authentication.

### Quick Start

```bash
/opt/spark/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
  --master armada://localhost:50051 \
  --deploy-mode cluster \
  --name my-secure-job \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.armada.container.image=armada-spark \
  --conf spark.armada.oauth.enabled=true \
  --conf spark.armada.oauth.clientId=spark-oauth-client \
  --conf spark.armada.oauth.clientSecret=your-secret \
  --conf spark.armada.oauth.issuerUrl=https://keycloak.example.com/realms/spark \
  --conf spark.armada.driver.ingress.enabled=true \
  --conf spark.armada.driver.ingress.tls.enabled=true \
  --conf spark.armada.driver.ingress.certName=my-tls-cert \
  local:///opt/spark/examples/jars/spark-examples.jar
```

**What happens:**
1. `oauth` sidecar container added to driver pod
2. Ingress → oauth2-proxy (port 4180) → authenticates user → Spark UI (localhost:4040)
3. oauth2-proxy terminates when driver completes

### Configuration Examples

See [OAuth2 Authentication Configuration](./architecture.md#oauth2-authentication-configuration) for all parameters.

**Using OIDC discovery:**
```bash
--conf spark.armada.oauth.enabled=true \
--conf spark.armada.oauth.clientId=my-client \
--conf spark.armada.oauth.clientSecret=my-secret \
--conf spark.armada.oauth.issuerUrl=https://provider.com/realms/spark \
--conf spark.armada.driver.ingress.enabled=true \
--conf spark.armada.driver.ingress.tls.enabled=true
```

**Manual endpoints (no discovery):**
```bash
--conf spark.armada.oauth.enabled=true \
--conf spark.armada.oauth.skipProviderDiscovery=true \
--conf spark.armada.oauth.loginUrl=https://provider.com/auth \
--conf spark.armada.oauth.redeemUrl=http://provider.svc.cluster.local/token \
--conf spark.armada.oauth.validateUrl=http://provider.svc.cluster.local/userinfo \
--conf spark.armada.oauth.jwksUrl=http://provider.svc.cluster.local/certs
```

**Use cluster-internal URLs** for `redeemUrl`/`validateUrl`/`jwksUrl`, external URL for `loginUrl`.

**Using K8s secrets (recommended):**
```bash
kubectl create secret generic spark-oauth-secret \
  --from-literal=client-secret=your-secret -n spark-jobs

--conf spark.armada.oauth.clientId=my-client \
--conf spark.armada.oauth.clientSecretK8s=spark-oauth-secret
```

---

## Troubleshooting

### 502 Bad Gateway after login

**Cause:** Spark UI not running (job finished too quickly or UI disabled)

**Check logs:**
```bash
kubectl logs -n <namespace> <driver-pod> -c oauth
```

Look for: `Error proxying to upstream server: dial tcp 127.0.0.1:4040: connect: connection refused`

**Solutions:**
- Use longer-running job (Spark Pi finishes in seconds)
- Spark UI has 90s delay after job completion by default
- Verify `spark.ui.enabled=true` (default)

### Authentication keeps redirecting

**Cause:** Cookie config or OIDC provider issues

**Solutions:**
```bash
# For HTTP (dev only):
--conf spark.armada.oauth.cookieSecure=false

# Check SameSite:
--conf spark.armada.oauth.cookieSamesite=lax

# Verify redirect URL matches OIDC provider config:
--conf spark.armada.oauth.redirectUrl=https://your-host/oauth2/callback
```

### TLS certificate errors

```bash
# Dev only:
--conf spark.armada.oauth.skipVerify=true

# Production - mount CA cert via job template, then:
--conf spark.armada.oauth.tls.caCertPath=/etc/ssl/certs/ca.crt
```

### Finding ingress URL

```bash
kubectl get ingress -n <namespace>
# Output: oauth-4180-armada-<job-id>-0.namespace.svc
```

---

## Best Practices

### Production
- Use OAuth with TLS: `spark.armada.oauth.enabled=true`, `spark.armada.driver.ingress.tls.enabled=true`
- Store secrets in K8s: `spark.armada.oauth.clientSecretK8s=my-secret`
- Restrict domains: `spark.armada.oauth.emailDomain=company.com`
- Cookie security: `spark.armada.oauth.cookieSecure=true`, `spark.armada.oauth.cookieSamesite=lax`

### Development
- Port-forward for quick access: `kubectl port-forward <pod> 4040:4040`
- Skip TLS verification: `spark.armada.oauth.skipVerify=true`, `spark.armada.oauth.insecureSkipIssuerVerification=true`

---

## Resources

- [OAuth2 Configuration Reference](./architecture.md#oauth2-authentication-configuration)
- [oauth2-proxy docs](https://oauth2-proxy.github.io/oauth2-proxy/)
- [Spark UI docs](https://spark.apache.org/docs/latest/web-ui.html)
