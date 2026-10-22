/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.inbound.solace;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.endpoint.protocol.PollingConstants;
import org.wso2.config.mapper.ConfigParser;

import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.*;
import static org.wso2.carbon.inbound.solace.SolaceUtils.parseBoolean;
import static org.wso2.carbon.inbound.solace.SolaceUtils.parseDouble;
import static org.wso2.carbon.inbound.solace.SolaceUtils.parseInt;
import static org.wso2.carbon.inbound.solace.SolaceUtils.parseLong;

public final class SolaceInboundConfig {

    private static final Log log = LogFactory.getLog(SolaceInboundConfig.class);

    private static final long DEFAULT_SETTLEMENT_TIMEOUT = Long.parseLong(DEFAULT_SETTLEMENT_TIMEOUT_MS);

    // Connection Configurations
    public final String host;
    public final String vpnName;
    public final String username;
    public final String apiProvidedUsername;
    public final String password;
    public final String clientName;
    public final int connectRetries;
    public final int connectRetriesPerHost;
    public final int reconnectRetries;
    public final int reconnectRetryWaitMillis;
    public final int connectionTimeoutMillis;

    // Authentication scheme
    public final String authScheme;
    public final String oauth2AccessToken;
    public final String oauth2IssuerIdentifier;
    public final String oidcIdToken;

    // SSL/TLS Configurations
    public final String sslTrustStorePath;
    public final String sslTrustStorePassword;
    public final String sslTrustStoreFormat;
    public final String sslKeyStorePath;
    public final String sslKeyStorePassword;
    public final String sslKeyStoreFormat;
    public final String sslKeyPassword;
    public final boolean sslValidateCertificate;
    public final boolean sslValidateCertificateDate;
    public final boolean generateReceiveTimestamps;

    // Destination Configurations
    public final String destinationType;
    public final String destinationName;
    public final String subscriptionType;
    public final String dteName;

    // Consumer Behavior Configurations
    public final boolean autoAck;
    public final int subAckWindowSize;
    public final String failureOutcome;
    public final boolean binaryPayloadAsBase64;
    public final String selector;

    // Queue provisioning
    public final boolean provisionDestination;
    public final String queueAccessType;
    public final String queuePermission;
    public final int queueQuotaMB;
    public final int queueMaxMsgSize;
    public final boolean queueRespectTTL;
    public final int queueMaxMsgRedelivery;

    // Ingestion Configurations
    public final String contentType;

    // 2.0.0 parameters
    public final long settlementTimeoutMillis;
    public final String settlementTimeoutSource;
    public final int concurrentConsumers;
    public final long drainTimeoutMillis;
    public final String drainOutcome;
    public final long redeliveryInitialDelayMillis;
    public final double redeliveryProgression;
    public final long redeliveryMaxDelayMillis;
    public final int maxRedeliveries;
    public final long recoveryInitialDelayMillis;
    public final double recoveryProgression;
    public final long recoveryMaxDelayMillis;
    public final int recoveryMaxAttempts;
    public final String sharedSubscriptionGroup;
    public final boolean transacted;
    public final boolean clientNameStrict;
    public final boolean sslValidateHostname;
    public final int compressionLevel;
    public final long keepAliveIntervalMillis;
    public final int keepAliveLimit;
    public final Path oauth2TokenFile;
    public final String requireSettlementOutcomes;

    // Execution behaviour
    public final boolean sequential;
    public final boolean coordination;

    // Derived
    public final boolean direct;
    public final boolean guaranteed;
    public final int highWaterMark;
    public final int lowWaterMark;

    public final List<String> errors;

    public static SolaceInboundConfig parse(Properties properties, String endpointName, boolean coordination,
                                            boolean sequential) {
        return new SolaceInboundConfig(properties, endpointName, coordination, sequential);
    }

    private SolaceInboundConfig(Properties properties, String endpointName, boolean coordination,
                                boolean sequential) {
        List<String> errors = new ArrayList<>();

        // Connection
        this.host = properties.getProperty(HOST);
        this.vpnName = properties.getProperty(VPN_NAME, "default");
        this.username = properties.getProperty(USERNAME);
        String providedUsername = properties.getProperty(API_PROVIDED_USERNAME_PREFIXED);
        if (StringUtils.isBlank(providedUsername)) {
            providedUsername = properties.getProperty(API_PROVIDED_USERNAME);
        }
        this.apiProvidedUsername = providedUsername;
        this.password = properties.getProperty(PASSWORD);
        this.clientName = properties.getProperty(CLIENT_NAME);

        // Authentication scheme
        this.authScheme = properties.getProperty(AUTH_SCHEME, AUTH_SCHEME_BASIC);
        this.oauth2AccessToken = properties.getProperty(OAUTH2_ACCESS_TOKEN);
        this.oauth2IssuerIdentifier = properties.getProperty(OAUTH2_ISSUER_IDENTIFIER);
        this.oidcIdToken = properties.getProperty(OIDC_ID_TOKEN);

        // Connection retry
        this.connectionTimeoutMillis = parseInt(properties.getProperty(CONNECTION_TIMEOUT),
                CONNECTION_TIMEOUT, 30000, errors);
        this.connectRetries = parseInt(properties.getProperty(CONNECT_RETRIES),
                CONNECT_RETRIES, 3, errors);
        this.connectRetriesPerHost = parseInt(properties.getProperty(CONNECT_RETRIES_PER_HOST),
                CONNECT_RETRIES_PER_HOST, 3, errors);
        this.reconnectRetries = parseInt(properties.getProperty(RECONNECT_RETRIES),
                RECONNECT_RETRIES, -1, errors);
        if (reconnectRetries < -1) {
            errors.add(RECONNECT_RETRIES + " must be at least -1, got " + reconnectRetries);
        }
        this.reconnectRetryWaitMillis = parseInt(properties.getProperty(RECONNECT_RETRY_WAIT),
                RECONNECT_RETRY_WAIT, 3000, errors);

        // SSL/TLS
        this.sslTrustStorePath = properties.getProperty(SSL_TRUST_STORE_PATH);
        this.sslTrustStorePassword = properties.getProperty(SSL_TRUST_STORE_PASSWORD);
        this.sslTrustStoreFormat = properties.getProperty(SSL_TRUST_STORE_FORMAT);
        this.sslKeyStorePath = properties.getProperty(SSL_KEY_STORE_PATH);
        this.sslKeyStorePassword = properties.getProperty(SSL_KEY_STORE_PASSWORD);
        this.sslKeyStoreFormat = properties.getProperty(SSL_KEY_STORE_FORMAT);
        this.sslKeyPassword = properties.getProperty(SSL_KEY_PASSWORD);
        this.sslValidateCertificate = parseBoolean(properties.getProperty(SSL_VALIDATE_CERTIFICATE),
                SSL_VALIDATE_CERTIFICATE, true, errors);
        this.sslValidateCertificateDate = parseBoolean(properties.getProperty(SSL_VALIDATE_CERTIFICATE_DATE),
                SSL_VALIDATE_CERTIFICATE_DATE, true, errors);
        this.generateReceiveTimestamps = parseBoolean(properties.getProperty(GENERATE_RECEIVE_TIMESTAMPS),
                GENERATE_RECEIVE_TIMESTAMPS, false, errors);

        // Destination
        this.destinationType = properties.getProperty(DESTINATION_TYPE);
        this.destinationName = properties.getProperty(DESTINATION_NAME);
        this.subscriptionType = properties.getProperty(SUBSCRIPTION_TYPE, SUBSCRIPTION_TYPE_DIRECT);
        this.dteName = properties.getProperty(DTE_NAME);
        this.direct = DESTINATION_TYPE_TOPIC.equalsIgnoreCase(destinationType)
                && SUBSCRIPTION_TYPE_DIRECT.equalsIgnoreCase(subscriptionType);
        this.guaranteed = !direct;

        // Consumer behaviour
        this.autoAck = parseBoolean(properties.getProperty(AUTO_ACK), AUTO_ACK, true, errors);
        this.subAckWindowSize = parseInt(properties.getProperty(SUB_ACK_WINDOW_SIZE),
                SUB_ACK_WINDOW_SIZE, 255, errors);
        if (subAckWindowSize < 1 || subAckWindowSize > 255) {
            errors.add(SUB_ACK_WINDOW_SIZE + " must be between 1 and 255, got " + subAckWindowSize);
        }
        this.highWaterMark = subAckWindowSize;
        this.failureOutcome = enumValue(properties.getProperty(FAILURE_OUTCOME), FAILURE_OUTCOME,
                FAILURE_OUTCOME_FAILED, errors, FAILURE_OUTCOME_NONE, FAILURE_OUTCOME_FAILED, FAILURE_OUTCOME_REJECTED);
        this.binaryPayloadAsBase64 = parseBoolean(properties.getProperty(BINARY_PAYLOAD_AS_BASE64),
                BINARY_PAYLOAD_AS_BASE64, false, errors);
        this.selector = properties.getProperty(SELECTOR);

        // Queue provisioning
        this.provisionDestination = parseBoolean(properties.getProperty(PROVISION_DESTINATION),
                PROVISION_DESTINATION, false, errors);
        this.queueAccessType = properties.getProperty(QUEUE_ACCESS_TYPE, QUEUE_ACCESS_EXCLUSIVE);
        this.queuePermission = properties.getProperty(QUEUE_PERMISSION);
        this.queueQuotaMB = parseInt(properties.getProperty(QUEUE_QUOTA_MB), QUEUE_QUOTA_MB, 0, errors);
        this.queueMaxMsgSize = parseInt(properties.getProperty(QUEUE_MAX_MSG_SIZE), QUEUE_MAX_MSG_SIZE, 0, errors);
        this.queueRespectTTL = parseBoolean(properties.getProperty(QUEUE_RESPECT_TTL),
                QUEUE_RESPECT_TTL, false, errors);
        this.queueMaxMsgRedelivery = parseInt(properties.getProperty(QUEUE_MAX_MSG_REDELIVERY),
                QUEUE_MAX_MSG_REDELIVERY, 0, errors);

        // Ingestion
        this.contentType = properties.getProperty(CONTENT_TYPE);

        // Settlement timeout resolution
        long settlement;
        String source;
        String parameterValue = properties.getProperty(SETTLEMENT_TIMEOUT);
        if (StringUtils.isNotBlank(parameterValue)) {
            settlement = parseLong(parameterValue, SETTLEMENT_TIMEOUT, DEFAULT_SETTLEMENT_TIMEOUT, errors);
            source = "parameter";
        } else {
            Object toml = null;
            try {
                toml = ConfigParser.getParsedConfigs().get(TOML_MESSAGING_CALLBACK + "." + TOML_SETTLEMENT_TIMEOUT_KEY);
            } catch (Throwable t) {
                // ConfigParser is absent or not initialised, so the default applies. Nothing is logged here.
            }
            if (toml != null) {
                source = "deployment.toml";
                long tomlTimeout;
                try {
                    tomlTimeout = Long.parseLong(toml.toString().trim());
                } catch (NumberFormatException e) {
                    errors.add("invalid value for " + TOML_MESSAGING_CALLBACK + "." + TOML_SETTLEMENT_TIMEOUT_KEY
                            + ": " + toml);
                    tomlTimeout = DEFAULT_SETTLEMENT_TIMEOUT;
                }
                settlement = tomlTimeout;
            } else {
                settlement = DEFAULT_SETTLEMENT_TIMEOUT;
                source = "default";
            }
        }
        if (settlement < 1000) {
            errors.add(SETTLEMENT_TIMEOUT + " must be at least 1000 ms, got " + settlement);
        }
        this.settlementTimeoutMillis = settlement;
        this.settlementTimeoutSource = source;

        this.concurrentConsumers = parseInt(properties.getProperty(CONCURRENT_CONSUMERS),
                CONCURRENT_CONSUMERS, 1, errors);
        if (concurrentConsumers < 1 || concurrentConsumers > 1024) {
            errors.add(CONCURRENT_CONSUMERS + " must be between 1 and 1024, got " + concurrentConsumers);
        }
        this.lowWaterMark = Math.min(concurrentConsumers, highWaterMark - 1);

        this.drainTimeoutMillis = parseLong(properties.getProperty(DRAIN_TIMEOUT), DRAIN_TIMEOUT, 30000, errors);
        if (drainTimeoutMillis < MIN_DRAIN_TIMEOUT_MS) {
            errors.add(DRAIN_TIMEOUT + " must be at least " + MIN_DRAIN_TIMEOUT_MS + " ms (" + SETTLE_BUDGET_MS
                    + " ms of it is the settle budget), got " + drainTimeoutMillis);
        }
        this.drainOutcome = enumValue(properties.getProperty(DRAIN_OUTCOME), DRAIN_OUTCOME,
                DRAIN_OUTCOME_NONE, errors, DRAIN_OUTCOME_NONE, DRAIN_OUTCOME_REJECTED);

        this.redeliveryInitialDelayMillis = parseLong(properties.getProperty(REDELIVERY_INITIAL_DELAY),
                REDELIVERY_INITIAL_DELAY, 1000, errors);
        this.redeliveryProgression = parseDouble(properties.getProperty(REDELIVERY_PROGRESSION),
                REDELIVERY_PROGRESSION, 2.0, errors);
        this.redeliveryMaxDelayMillis = parseLong(properties.getProperty(REDELIVERY_MAX_DELAY),
                REDELIVERY_MAX_DELAY, 60000, errors);
        checkBackoff(REDELIVERY_INITIAL_DELAY, redeliveryInitialDelayMillis, REDELIVERY_PROGRESSION,
                redeliveryProgression, REDELIVERY_MAX_DELAY, redeliveryMaxDelayMillis, errors);
        this.maxRedeliveries = parseInt(properties.getProperty(MAX_REDELIVERIES), MAX_REDELIVERIES, -1, errors);
        if (maxRedeliveries < -1) {
            errors.add(MAX_REDELIVERIES + " must be at least -1, got " + maxRedeliveries);
        }

        this.recoveryInitialDelayMillis = parseLong(properties.getProperty(RECOVERY_INITIAL_DELAY),
                RECOVERY_INITIAL_DELAY, 1000, errors);
        this.recoveryProgression = parseDouble(properties.getProperty(RECOVERY_PROGRESSION),
                RECOVERY_PROGRESSION, 2.0, errors);
        this.recoveryMaxDelayMillis = parseLong(properties.getProperty(RECOVERY_MAX_DELAY),
                RECOVERY_MAX_DELAY, 60000, errors);
        checkBackoff(RECOVERY_INITIAL_DELAY, recoveryInitialDelayMillis, RECOVERY_PROGRESSION,
                recoveryProgression, RECOVERY_MAX_DELAY, recoveryMaxDelayMillis, errors);
        this.recoveryMaxAttempts = parseInt(properties.getProperty(RECOVERY_MAX_ATTEMPTS),
                RECOVERY_MAX_ATTEMPTS, -1, errors);
        if (recoveryMaxAttempts < -1) {
            errors.add(RECOVERY_MAX_ATTEMPTS + " must be at least -1, got " + recoveryMaxAttempts);
        }

        String group = properties.getProperty(SHARED_SUBSCRIPTION_GROUP);
        this.sharedSubscriptionGroup = StringUtils.isBlank(group) ? "" : group.trim();
        if (sharedSubscriptionGroup.indexOf('/') >= 0 || sharedSubscriptionGroup.indexOf('#') >= 0
                || sharedSubscriptionGroup.indexOf('*') >= 0) {
            errors.add(SHARED_SUBSCRIPTION_GROUP + " must not contain '/', '#' or '*', got '"
                    + sharedSubscriptionGroup + "'");
        }

        this.transacted = parseBoolean(properties.getProperty(TRANSACTED), TRANSACTED, false, errors);
        if (transacted && autoAck) {
            errors.add("solace.transacted=true requires solace.autoAck=false");
        }
        if (transacted && DESTINATION_TYPE_TOPIC.equalsIgnoreCase(destinationType)) {
            errors.add("solace.transacted=true is supported on a queue only");
        }

        this.clientNameStrict = parseBoolean(properties.getProperty(CLIENT_NAME_STRICT),
                CLIENT_NAME_STRICT, false, errors);
        if (clientNameStrict && StringUtils.isNotBlank(clientName)) {
            int bytes = clientName.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > MAX_CLIENT_NAME_BYTES) {
                errors.add(CLIENT_NAME + " must be at most " + MAX_CLIENT_NAME_BYTES + " bytes with "
                        + CLIENT_NAME_STRICT + "=true, got " + bytes + " bytes");
            }
        }
        this.sslValidateHostname = parseBoolean(properties.getProperty(SSL_VALIDATE_HOSTNAME),
                SSL_VALIDATE_HOSTNAME, true, errors);
        this.compressionLevel = parseInt(properties.getProperty(COMPRESSION_LEVEL), COMPRESSION_LEVEL, 0, errors);
        if (compressionLevel < 0 || compressionLevel > 9) {
            errors.add(COMPRESSION_LEVEL + " must be between 0 and 9, got " + compressionLevel);
        }
        this.keepAliveIntervalMillis = parseLong(properties.getProperty(KEEP_ALIVE_INTERVAL),
                KEEP_ALIVE_INTERVAL, -1, errors);
        if (keepAliveIntervalMillis != -1
                && (keepAliveIntervalMillis < 50 || keepAliveIntervalMillis > Integer.MAX_VALUE)) {
            errors.add(KEEP_ALIVE_INTERVAL + " must be at least 50 ms and at most " + Integer.MAX_VALUE
                    + ", got " + keepAliveIntervalMillis);
        }
        this.keepAliveLimit = parseInt(properties.getProperty(KEEP_ALIVE_LIMIT), KEEP_ALIVE_LIMIT, -1, errors);
        if (keepAliveLimit != -1 && keepAliveLimit < 1) {
            errors.add(KEEP_ALIVE_LIMIT + " must be at least 1, got " + keepAliveLimit);
        }

        String tokenFile = properties.getProperty(OAUTH2_TOKEN_FILE);
        Path tokenPath = null;
        if (StringUtils.isNotBlank(tokenFile)) {
            try {
                tokenPath = Paths.get(tokenFile.trim());
            } catch (InvalidPathException e) {
                errors.add("invalid value '" + tokenFile + "' for " + OAUTH2_TOKEN_FILE);
            }
            if (!AUTH_SCHEME_OAUTH2.equalsIgnoreCase(authScheme)) {
                errors.add(OAUTH2_TOKEN_FILE + " requires " + AUTH_SCHEME + "=" + AUTH_SCHEME_OAUTH2);
            }
        }
        this.oauth2TokenFile = tokenPath;

        this.requireSettlementOutcomes = enumValue(properties.getProperty(REQUIRE_SETTLEMENT_OUTCOMES),
                REQUIRE_SETTLEMENT_OUTCOMES, REQUIRE_OUTCOMES_AUTO, errors, REQUIRE_OUTCOMES_AUTO,
                REQUIRE_OUTCOMES_TRUE);

        // Connection parameter rules (the baseline validateConnectionParameters)
        if (StringUtils.isBlank(host)) {
            errors.add(HOST + " is required for Solace connection");
        }
        if (AUTH_SCHEME_OAUTH2.equalsIgnoreCase(authScheme)) {
            if (StringUtils.isBlank(oauth2AccessToken) && StringUtils.isBlank(oidcIdToken)
                    && oauth2TokenFile == null) {
                errors.add("OAUTH2 authentication requires '" + OAUTH2_ACCESS_TOKEN + "', '" + OIDC_ID_TOKEN
                        + "' or '" + OAUTH2_TOKEN_FILE + "'.");
            }
        } else if (AUTH_SCHEME_CLIENT_CERTIFICATE.equalsIgnoreCase(authScheme)) {
            if (StringUtils.isBlank(sslKeyStorePath)) {
                errors.add(SSL_KEY_STORE_PATH + " is required for CLIENT_CERTIFICATE authentication");
            }
        } else {
            // BASIC (default). A null username NPEs in JCSMP during login-frame build; password is
            // left to the broker to reject, since some setups allow username-only authentication.
            if (StringUtils.isBlank(username)) {
                errors.add(USERNAME + " is required for BASIC authentication");
            }
        }

        // Execution behaviour
        boolean sequentialProcessing = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(
                        properties.getProperty(PollingConstants.INBOUND_ENDPOINT_SEQUENTIAL)),
                sequential);
        this.coordination = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(
                        properties.getProperty(PollingConstants.INBOUND_COORDINATION)),
                coordination);

        // Client-acknowledgement settles each message based on the mediation outcome, which is
        // only known synchronously. Under async injection (sequential=false) the listener's
        // post-injection ack would run before mediation, then a connector operation acknowledgeMessage /
        // nackMessage could settle the same message from a worker thread. Solace treats a second
        // ack as a harmless no-op, but an ack followed by a nack is not: the message is already
        // removed as consumed, so the intended FAILED/REJECTED (redelivery / DMQ routing) outcome
        // is silently lost. Force sequential processing so the settle decision is always made from
        // a completed mediation. (Matches other MI inbounds — e.g. Kafka — which also pin
        // sequential for manual-commit/ack modes.)
        if (!autoAck && !sequentialProcessing) {
            log.warn("SolaceListener [" + endpointName + "]: autoAck=false requires sequential processing; "
                    + "overriding sequential=true to prevent an ack-then-nack double-settle race.");
            sequentialProcessing = true;
        }
        this.sequential = sequentialProcessing;

        this.errors = Collections.unmodifiableList(errors);
    }

    private static String enumValue(String value, String name, String defaultValue, List<String> errors,
                                    String... allowed) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        String upper = value.trim().toUpperCase(Locale.ROOT);
        if (Arrays.asList(allowed).contains(upper)) {
            return upper;
        }
        errors.add(name + " must be one of " + String.join(", ", allowed) + ", got '" + value + "'");
        return defaultValue;
    }

    private static void checkBackoff(String initialName, long initial, String progressionName, double progression,
                                     String maxName, long max, List<String> errors) {
        if (initial < 0) {
            errors.add(initialName + " must be at least 0, got " + initial);
        }
        if (progression < 1.0) {
            errors.add(progressionName + " must be at least 1.0, got " + progression);
        }
        if (max < initial) {
            errors.add(maxName + " must be at least " + initialName + " (" + initial + "), got " + max);
        }
    }
}
