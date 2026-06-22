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
package io.armadaproject.spark.connect.auth;

import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.RegisteredClaims;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.RSAKeyProvider;
import com.auth0.jwt.interfaces.Verification;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class JwtValidator {

    private static final Logger LOG = LoggerFactory.getLogger(JwtValidator.class);

    private final JWTVerifier verifier;
    private final String issuerUrl;
    private final String audience;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Upper bound on an OIDC discovery document; real docs are a few KB. */
    private static final long MAX_DISCOVERY_BYTES = 1_048_576L; // 1 MiB

    /** Package-private constructor for tests: pass a fully-built verifier. */
    JwtValidator(JWTVerifier verifier, String issuerUrl, String audience) {
        this.verifier  = verifier;
        this.issuerUrl = issuerUrl;
        this.audience  = audience;
    }

    /** Builds the verifier from already-parsed {@link AuthConfig} (issuer/jwks/audience). */
    JwtValidator(AuthConfig cfg) {
        String issuer = cfg.issuerUrl();
        // The issuer is the trust anchor (matched against the token's `iss`); require https
        // regardless of how the JWKS URL is sourced, so an http issuer can never pass silently.
        requireHttps(issuer, "OIDC issuer URL");

        String jwksUrl = resolveJwksUrl(issuer, cfg.jwksUrl());
        requireHttps(jwksUrl, "JWKS URL");

        JwkProvider jwkProvider;
        try {
            jwkProvider = new JwkProviderBuilder(new URL(jwksUrl))
                    .cached(10, 24, TimeUnit.HOURS)
                    .rateLimited(10, 1, TimeUnit.MINUTES)
                    .timeouts(5_000, 5_000)  // connect, read; in milliseconds
                    .build();
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Invalid jwks URL: " + jwksUrl, e);
        }

        Algorithm algorithm = Algorithm.RSA256(new RSAKeyProviderFromJwks(jwkProvider));
        // Require `exp` to be present: java-jwt only enforces expiry when the claim exists, so
        // a token minted without `exp` would otherwise be accepted forever.
        Verification builder =
                JWT.require(algorithm).withIssuer(issuer).withClaimPresence(RegisteredClaims.EXPIRES_AT);
        String aud = cfg.audience(); // null when unset/blank (normalized by AuthConfig)
        if (aud != null) {
            builder.withAudience(aud);
        } else {
            // Without an audience constraint, any token the issuer minted for any relying party is
            // accepted as long as its `sub` matches the owner. Set spark.armada.connect.oidc.audience
            // to bind tokens to this server.
            LOG.warn(
                    "{} is not set: tokens for ANY audience issued by {} will be accepted. "
                            + "Set {} to restrict tokens to this server.",
                    AuthConfig.AUDIENCE,
                    issuer,
                    AuthConfig.AUDIENCE);
        }

        this.verifier  = builder.build();
        this.issuerUrl = issuer;
        this.audience  = aud;
        LOG.info("JwtValidator initialized: issuer={}, jwks={}, audience={}",
                issuer, jwksUrl, aud == null ? "<none>" : aud);
    }

    public DecodedJWT verify(String token) throws JWTVerificationException {
        return verifier.verify(token);
    }

    public String issuerUrl() { return issuerUrl; }
    public String audience()  { return audience;  }

    /**
     * Resolve the JWKS endpoint URL. Returns {@code overrideJwksUrl} when set, otherwise
     * fetches {@code <issuer>/.well-known/openid-configuration} and reads its
     * {@code jwks_uri} field.
     */
    static String resolveJwksUrl(String issuerUrl, String overrideJwksUrl) {
        if (overrideJwksUrl != null && !overrideJwksUrl.isBlank()) {
            return overrideJwksUrl;
        }
        // Issuer https is enforced unconditionally by the constructor before we get here.
        String discoveryUrl = trimTrailingSlash(issuerUrl) + "/.well-known/openid-configuration";
        return fetchJwksUri(discoveryUrl);
    }

    /**
     * Fetch an OIDC discovery document and return its {@code jwks_uri}. Package-private so the
     * networked behavior (redirects, non-2xx, malformed body) can be exercised against a local
     * HTTP server in tests; scheme enforcement lives in the callers.
     */
    static String fetchJwksUri(String discoveryUrl) {
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .followRedirects(HttpClient.Redirect.NORMAL) // follow http->https & aliases, never downgrade
                    .build();
            HttpRequest req = HttpRequest.newBuilder(URI.create(discoveryUrl))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException(
                        "OIDC discovery returned HTTP " + resp.statusCode() + " from " + discoveryUrl);
            }
            // Cap the body so a hostile/misconfigured endpoint can't OOM us during startup.
            String body;
            try (InputStream in = resp.body()) {
                body = readDiscoveryBody(in, discoveryUrl);
            }
            return parseJwksUri(body);
        } catch (IOException e) {
            throw new IllegalStateException("OIDC discovery failed for " + discoveryUrl, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("OIDC discovery interrupted for " + discoveryUrl, e);
        }
    }

    /**
     * Read a discovery body, enforcing {@link #MAX_DISCOVERY_BYTES}. Package-private so the cap is
     * unit-testable directly, without streaming a large body through the JDK HTTP client.
     */
    static String readDiscoveryBody(InputStream in, String sourceUrl) throws IOException {
        byte[] bytes = in.readNBytes((int) MAX_DISCOVERY_BYTES + 1);
        if (bytes.length > MAX_DISCOVERY_BYTES) {
            throw new IllegalStateException(
                    "OIDC discovery document from " + sourceUrl + " exceeds " + MAX_DISCOVERY_BYTES + " bytes");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static String parseJwksUri(String discoveryJson) {
        try {
            JsonNode root = MAPPER.readTree(discoveryJson);
            JsonNode field = root.get("jwks_uri");
            if (field == null || !field.isTextual() || field.asText().isBlank()) {
                throw new IllegalStateException("OIDC discovery doc has no jwks_uri");
            }
            return field.asText();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse OIDC discovery doc", e);
        }
    }

    private static String trimTrailingSlash(String s) {
        return (s != null && s.endsWith("/")) ? s.substring(0, s.length() - 1) : s;
    }

    /**
     * Reject non-HTTPS URLs. Fetching discovery docs or signing keys over plaintext would
     * let a man-in-the-middle substitute keys and forge tokens, defeating verification.
     */
    private static void requireHttps(String url, String label) {
        String scheme;
        try {
            scheme = URI.create(url).getScheme();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Invalid " + label + ": " + url, e);
        }
        if (!"https".equalsIgnoreCase(scheme)) {
            throw new IllegalStateException(
                    label + " must use https to prevent key-substitution attacks: " + url);
        }
    }

    private static final class RSAKeyProviderFromJwks implements RSAKeyProvider {
        private final JwkProvider jwkProvider;
        RSAKeyProviderFromJwks(JwkProvider jwkProvider) {
            this.jwkProvider = jwkProvider;
        }

        @Override
        public RSAPublicKey getPublicKeyById(String keyId) {
            try {
                return (RSAPublicKey) jwkProvider.get(keyId).getPublicKey();
            } catch (Exception e) {
                throw new JWTVerificationException(
                        "Failed to fetch public key for kid=" + keyId, e);
            }
        }

        @Override public RSAPrivateKey getPrivateKey() { return null; }
        @Override public String getPrivateKeyId() { return null; }
    }
}
