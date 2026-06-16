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

import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtAuthInterceptor implements ServerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(JwtAuthInterceptor.class);

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String BEARER_PREFIX = "Bearer ";

    private final JwtValidator validator;
    private final String owner;

    static final String CONF_OWNER = "spark.armada.connect.owner";

    /** Reflection-friendly constructor. Reads the owner from the SparkConf. */
    public JwtAuthInterceptor(SparkConf conf) {
        String configuredOwner = conf.get(CONF_OWNER, null);
        if (configuredOwner == null || configuredOwner.isBlank()) {
            throw new IllegalStateException(
                    CONF_OWNER + " must be set to use JwtAuthInterceptor");
        }
        this.owner = configuredOwner;
        this.validator = new JwtValidator(conf);
        LOG.info("JwtAuthInterceptor initialized: owner={}", owner);
    }

    /** Package-private constructor for tests. */
    JwtAuthInterceptor(JwtValidator validator, String owner) {
        if (owner == null || owner.isBlank()) {
            throw new IllegalStateException(
                    "owner must not be null or blank; set " + CONF_OWNER);
        }
        this.validator = validator;
        this.owner = owner;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String authHeader = headers.get(AUTH_KEY);
        if (authHeader == null) {
            return reject(call, Status.UNAUTHENTICATED, "Missing Authorization header");
        }
        String token = stripBearer(authHeader);
        if (token == null) {
            return reject(call, Status.UNAUTHENTICATED, "Authorization header is not a bearer token");
        }

        DecodedJWT jwt;
        try {
            jwt = validator.verify(token);
        } catch (JWTVerificationException e) {
            LOG.debug("JWT verification failed: {}", e.getMessage());
            return reject(call, Status.UNAUTHENTICATED, "JWT verification failed");
        }

        // Identity is the IdP-assigned `sub` claim (immutable, not self-assertable).
        String identity = jwt.getSubject();
        if (identity == null || identity.isBlank()) {
            return reject(call, Status.UNAUTHENTICATED, "Token has no subject claim");
        }
        if (!owner.equals(identity)) {
            LOG.warn("Access denied: subject '{}' does not match owner '{}'", identity, owner);
            return reject(call, Status.PERMISSION_DENIED,
                    "Caller is not the owner of this Spark Connect server");
        }

        LOG.debug("Authenticated request from owner '{}'", identity);
        return next.startCall(call, headers);
    }

    private static String stripBearer(String header) {
        if (header.length() < BEARER_PREFIX.length()) return null;
        String prefix = header.substring(0, BEARER_PREFIX.length());
        if (!prefix.equalsIgnoreCase(BEARER_PREFIX)) return null;
        String token = header.substring(BEARER_PREFIX.length()).trim();
        return token.isEmpty() ? null : token;
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> reject(
            ServerCall<ReqT, RespT> call, Status status, String message) {
        call.close(status.withDescription(message), new Metadata());
        return new ServerCall.Listener<ReqT>() {};
    }
}
