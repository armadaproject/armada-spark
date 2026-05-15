# Troubleshooting

## 502 Bad Gateway after login

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

## Authentication keeps redirecting

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

## Finding ingress URL

In Lookout, under Result tab, as soon as a Job is leased to a Cluster and bound to a Node, the Ingress URL will be accessible in that tab.

Or alternatively, the Ingress URL can be looked up by fetching the Ingress from the namespace where the Job is scheduled.
```bash
kubectl get ingress -n <namespace>
# Output: oauth-4180-armada-<job-id>-0.namespace.svc
```
