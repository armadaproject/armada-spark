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

import org.apache.spark.SparkConf;

/**
 * Parsed and validated Spark Connect JWT-auth configuration. All {@code spark.armada.connect.*}
 * keys are defined here so they have a single source of truth, and {@link #from(SparkConf)}
 * applies the read/trim/required rules in one place. Optional values are {@code null} when unset
 * or blank.
 *
 * <p>Kept as a plain final class (not a {@code record}) because the build matrix compiles Java at
 * levels older than 16 for some Spark/Scala combinations.
 */
final class AuthConfig {

    private static final String PREFIX = "spark.armada.connect.";
    static final String OWNER    = PREFIX + "owner";
    static final String ISSUER   = PREFIX + "oidc.issuerUrl";
    static final String JWKS     = PREFIX + "oidc.jwksUrl";
    static final String AUDIENCE = PREFIX + "oidc.audience";

    private final String owner;
    private final String issuerUrl;
    private final String jwksUrl;
    private final String audience;

    private AuthConfig(String owner, String issuerUrl, String jwksUrl, String audience) {
        this.owner     = owner;
        this.issuerUrl = issuerUrl;
        this.jwksUrl   = jwksUrl;
        this.audience  = audience;
    }

    /** Read and validate the connect-auth configuration from a SparkConf. */
    static AuthConfig from(SparkConf conf) {
        return new AuthConfig(
                required(conf, OWNER),
                required(conf, ISSUER),
                optional(conf, JWKS),
                optional(conf, AUDIENCE));
    }

    String owner()     { return owner;     }
    String issuerUrl() { return issuerUrl; }
    String jwksUrl()   { return jwksUrl;   }
    String audience()  { return audience;  }

    private static String required(SparkConf conf, String key) {
        String value = conf.get(key, null);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(key + " must be set");
        }
        return value.trim();
    }

    private static String optional(SparkConf conf, String key) {
        String value = conf.get(key, null);
        return (value == null || value.isBlank()) ? null : value.trim();
    }
}
