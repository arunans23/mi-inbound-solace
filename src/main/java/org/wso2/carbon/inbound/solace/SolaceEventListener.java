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

import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageProducer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumer;
import org.wso2.carbon.inbound.solace.hooks.Hooks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Solace inbound endpoint listener that consumes messages from a Solace Event Broker.
 *
 * <p>Supports three destination types controlled by the {@code solace.destinationType} parameter:
 * <ul>
 *   <li><b>QUEUE</b> — Guaranteed point-to-point delivery from a provisioned queue.</li>
 *   <li><b>TOPIC (Direct)</b> — Non-persistent fanout to all active topic subscribers.
 *       Accepts comma-separated topic strings for multiple subscriptions.</li>
 *   <li><b>TOPIC (Durable)</b> — Persistent topic delivery via a Durable Topic Endpoint (DTE).</li>
 * </ul>
 */
public class SolaceEventListener extends GenericEventBasedConsumer {

    private static final Log log = LogFactory.getLog(SolaceEventListener.class);

    enum State { STOPPED, CONNECTING, RUNNING, DRAINING, RECOVERING, FAILED }

    final String name;
    final SolaceInboundConfig config;
    final Counters counters = new Counters();
    final HealthReporter health;
    private final Object lifecycleLock = new Object();
    private State state = State.STOPPED;
    volatile Generation current;
    private int generationCounter = 0;
    private volatile ScheduledThreadPoolExecutor lifecycleExecutor;
    private volatile Closer closer;
    private final String nodeId;
    private final String nodeIdSource;
    final SolaceInjectHandler injectHandler;
    final OAuthTokenSource tokenSource;
    private final AtomicBoolean loggedDirectNoSettle = new AtomicBoolean();
    final AtomicBoolean loggedNonDmqReject = new AtomicBoolean();
    final AtomicBoolean loggedDeliveryCountOff = new AtomicBoolean();

    /**
     * @param properties
     * @param name
     * @param synapseEnvironment
     * @param injectingSeq
     * @param onErrorSeq
     * @param coordination
     * @param sequential
     */
    public SolaceEventListener(Properties properties, String name,
                               SynapseEnvironment synapseEnvironment,
                               String injectingSeq, String onErrorSeq,
                               boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, injectingSeq, onErrorSeq,
                coordination, sequential);
        this.name = name;
        this.config = SolaceInboundConfig.parse(properties, name, coordination, sequential);
        this.sequential = config.sequential;
        this.coordination = config.coordination;
        ClientNames.NodeId node = ClientNames.nodeId();
        this.nodeId = node.id;
        this.nodeIdSource = node.source;
        this.injectHandler = new SolaceInjectHandler(injectingSeq, onErrorSeq, synapseEnvironment, true,
                config.contentType, config.binaryPayloadAsBase64, name);
        this.tokenSource = new OAuthTokenSource(config.oauth2TokenFile, config.oauth2AccessToken, name);
        this.health = new HealthReporter(this);
    }

    @Override
    public void listen() {
        List<String> errors = new ArrayList<>(config.errors);
        if (!config.autoAck && config.direct && loggedDirectNoSettle.compareAndSet(false, true)) {
            log.info("SolaceListener [" + name + "] Direct topic with autoAck=false: the settlement decision bounds "
                    + "the work but nothing is settled on a Direct topic");
        }
        String guard = VersionGuard.check(synapseEnvironment, name);
        if (guard != null) {
            errors.add(guard);
        }
        if (!errors.isEmpty()) {
            synchronized (lifecycleLock) {
                if (state == State.STOPPED || state == State.FAILED) {
                    state = State.FAILED;
                    health.lifecycle(state);
                    log.error("SolaceListener [" + name + "] cannot start: " + String.join("; ", errors));
                    health.lastError(String.join("; ", errors));
                }
            }
            return;
        }
        if (!config.autoAck) {
            SequenceWalker.warnIfNoSettlementOperation(synapseEnvironment, name, injectingSeq, onErrorSeq,
                    config.settlementTimeoutMillis);
        }
        if (config.transacted) {
            if (properties.containsKey(SolaceInboundConstants.MAX_REDELIVERIES)) {
                log.warn("SolaceListener [" + name + "] solace.maxRedeliveries is ignored in transacted mode; the "
                        + "queue's maximum redelivery count and DMQ are the cutoff");
            }
            log.warn("SolaceListener [" + name + "] transacted mode relies on the queue's maximum redelivery count, "
                    + "a DMQ and max delivered unacknowledged messages per flow set to 1 for poison messages; this "
                    + "endpoint cannot verify the queue's settings through JCSMP, and provisioning with 'ignore "
                    + "already exists' does not change an existing queue; confirm them on the broker");
        }
        if (config.clientNameStrict) {
            // Strict mode sends the name verbatim, as 1.x did, and a duplicate name loops on evictions (A43).
            log.warn("SolaceListener [" + name + "] clientNameStrict=true: the client name '" + config.clientName
                    + "' is sent as is; a second session with this name on the broker evicts this one and the two "
                    + "evict each other in turn; keep it unique per node");
        }
        Generation generation;
        Hooks.get().point("listen.beforeTransition", this);
        synchronized (lifecycleLock) {
            if (state != State.STOPPED && state != State.FAILED) {
                if (log.isDebugEnabled()) {
                    log.debug("SolaceListener [" + name + "] listen() ignored, state=" + state);
                }
                return;
            }
            if (lifecycleExecutor == null || lifecycleExecutor.isShutdown()) {
                lifecycleExecutor = newLifecycleExecutor();
            }
            if (closer == null || closer.exiting) {
                closer = new Closer(name, health);
            }
            state = State.CONNECTING;
            generation = new Generation(++generationCounter, lifecycleExecutor, closer, config);
            current = generation;
            health.lifecycle(state);
            final Generation submitted = generation;
            try {
                submitted.lifecycleExecutor.execute(() -> connect(submitted, 1));
            } catch (RejectedExecutionException e) {
                retire(submitted);
                state = State.STOPPED;
                health.lifecycle(state);
                log.error("SolaceListener [" + name + "] could not submit the connect of generation " + submitted.id
                        + ": lifecycle executor shut down");
                return;
            }
        }
        log.info("SolaceListener [" + name + "] starting generation " + generation.id + " (node id " + nodeId + " from "
                + nodeIdSource + ", settlementTimeout " + config.settlementTimeoutMillis + " ms from "
                + config.settlementTimeoutSource + ", concurrentConsumers " + config.concurrentConsumers
                + ", window " + config.highWaterMark + "/" + config.lowWaterMark + ", coordination " + coordination
                + ")");
    }

    private ScheduledThreadPoolExecutor newLifecycleExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                new NamedThreadFactory("solace-lifecycle-" + name, true));
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    void connect(Generation generation, int attempt) {
        synchronized (lifecycleLock) {
            if (generation.retired) {
                return;
            }
            generation.claimed = true;
        }
        try {
            Hooks.get().point("connect.beforeBuild", generation);
            buildResources(generation);
            Hooks.get().point("connect.beforeBarrier", generation);
            synchronized (lifecycleLock) {
                if (generation.retired || (state != State.CONNECTING && state != State.RECOVERING)) {
                    generation.closer.submit(generation, () -> closeAll(generation));
                    return;
                }
                state = State.RUNNING;
                generation.active = true;
                health.lifecycle(state);
                health.connected(generation);
                counters.currentGeneration(generation);
                try {
                    generation.binding.start();
                } catch (JCSMPException e) {
                    generation.active = false;
                    throw e;
                }
            }
            log.info("SolaceListener [" + name + "] generation " + generation.id + " started: "
                    + generation.binding.describe());
        } catch (Exception e) {
            generation.closer.submit(generation, () -> closeAll(generation));
            synchronized (lifecycleLock) {
                if (generation.retired) {
                    return;
                }
                generation.retired = true;
                generation.active = false;
                if (e instanceof ConfigurationFailure) {
                    state = State.FAILED;
                    health.lifecycle(state);
                    health.lastError(e);
                    log.error("SolaceListener [" + name + "] cannot start: " + e.getMessage());
                    return;
                }
                if (config.recoveryMaxAttempts >= 0 && attempt >= config.recoveryMaxAttempts) {
                    state = State.FAILED;
                    health.lifecycle(state);
                    health.lastError(e);
                    log.error("SolaceListener [" + name + "] connection attempt " + attempt
                            + " failed and the retry budget is exhausted; the endpoint is FAILED until it is "
                            + "resumed or redeployed", e);
                    return;
                }
                long delay = DeliveryProcessor.backoff(config.recoveryInitialDelayMillis, config.recoveryProgression,
                        config.recoveryMaxDelayMillis, attempt);
                state = State.RECOVERING;
                health.lifecycle(state);
                health.lastError(e);
                log.warn("SolaceListener [" + name + "] connection attempt " + attempt + " failed; next attempt in "
                        + delay + " ms: " + e.getMessage(), e);
                Generation next = new Generation(++generationCounter, generation.lifecycleExecutor, generation.closer,
                        config);
                current = next;
                next.scheduled = next.lifecycleExecutor.schedule(() -> connect(next, attempt + 1), delay,
                        TimeUnit.MILLISECONDS);
            }
        } catch (Error e) {
            generation.closer.submit(generation, () -> closeAll(generation));
            synchronized (lifecycleLock) {
                if (!generation.retired) {
                    generation.retired = true;
                    generation.active = false;
                    state = State.FAILED;
                    health.lifecycle(state);
                    health.lastError(e);
                }
            }
            log.error("SolaceListener [" + name + "] generation " + generation.id
                    + " build failed with an error; the endpoint is FAILED until it is resumed or redeployed", e);
            throw e;
        }
    }

    void terminalFailure(Generation generation, String what, Exception cause) {
        if (generation != current) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceListener [" + name + "] " + what + " from retired generation " + generation.id
                        + " ignored");
            }
            return;
        }
        health.lastError(what + (cause != null ? ": " + cause.getMessage() : ""));
        try {
            generation.lifecycleExecutor.execute(() -> recover(generation));
        } catch (RejectedExecutionException e) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceListener [" + name + "] recovery of generation " + generation.id
                        + " not submitted: lifecycle executor shut down");
            }
        }
    }

    // A parked message belongs to the flow that delivered it. Once that flow leaves the connected state the broker
    // redelivers every unacknowledged message on the rebind, so the parked copies are stale (A44). JCSMP thread,
    // no JCSMP call. stopped is left as it is, the next worker that finishes restarts the flow (1.7).
    void discardParked(Generation generation, String why) {
        int parked;
        synchronized (generation.admission) {
            parked = generation.pending.size();
            if (parked == 0) {
                return;
            }
            generation.pending.clear();
            counters.dropped.add(parked);
            counters.gauge(generation).pending.set(0);
        }
        log.info("SolaceListener [" + name + "] discarded " + parked + " parked messages on " + why + " (generation "
                + generation.id + "): the broker redelivers them after the rebind");
    }

    void recover(Generation generation) {
        synchronized (lifecycleLock) {
            if (current != generation || generation.retired || state != State.RUNNING) {
                return;
            }
            state = State.RECOVERING;
            health.lifecycle(state);
        }
        log.warn("SolaceListener [" + name + "] generation " + generation.id + " is recovering: draining up to "
                + config.drainTimeoutMillis + " ms");
        DrainController.drain(this, generation, System.currentTimeMillis() + config.drainTimeoutMillis);
        Hooks.get().point("recover.afterDrain", generation);
        synchronized (lifecycleLock) {
            if (current != generation || !retire(generation)) {
                return;
            }
        }
        generation.closer.submit(generation, () -> closeAll(generation));
        Generation next;
        synchronized (lifecycleLock) {
            if (current != generation || state != State.RECOVERING) {
                return;
            }
            state = State.CONNECTING;
            health.lifecycle(state);
            next = new Generation(++generationCounter, generation.lifecycleExecutor, generation.closer, config);
            current = next;
        }
        connect(next, 1);
    }

    private boolean retire(Generation generation) {
        if (generation.retired) {
            return false;
        }
        generation.retired = true;
        generation.active = false;
        ExecutorService workers = generation.workers;
        if (workers != null) {
            workers.shutdown();
        }
        if (generation.scheduled != null) {
            generation.scheduled.cancel(false);
        }
        if (!generation.claimed) {
            generation.closer.detach(generation);
        }
        return true;
    }

    public void pause() {
        Generation generation;
        synchronized (lifecycleLock) {
            if (state == State.STOPPED || state == State.FAILED) {
                return;
            }
            generation = current;
            if (!generation.active) {
                retire(generation);
                state = State.STOPPED;
                health.lifecycle(state);
                log.info("SolaceListener [" + name + "] stopped while generation " + generation.id
                        + " was connecting; the attempt closes what it built");
                return;
            }
            state = State.DRAINING;
            health.lifecycle(state);
        }
        log.info("SolaceListener [" + name + "] pausing: draining generation " + generation.id + " for up to "
                + config.drainTimeoutMillis + " ms");
        DrainController.drain(this, generation, System.currentTimeMillis() + config.drainTimeoutMillis);
        Hooks.get().point("pause.afterDrain", generation);
        boolean owner;
        synchronized (lifecycleLock) {
            if (current != generation) {
                return;
            }
            owner = retire(generation);
            state = State.STOPPED;
            health.lifecycle(state);
        }
        if (owner) {
            generation.closer.submit(generation, () -> closeAll(generation));
        }
        log.info("SolaceListener [" + name + "] paused; generation " + generation.id
                + " is closing on the closer thread");
    }

    public void resume() {
        listen();
    }

    @Override
    public void destroy() {
        ScheduledThreadPoolExecutor capturedExecutor;
        Closer capturedCloser;
        synchronized (lifecycleLock) {
            capturedExecutor = lifecycleExecutor;
            capturedCloser = closer;
        }
        pause();
        synchronized (lifecycleLock) {
            if (state != State.STOPPED && state != State.FAILED) {
                log.info("SolaceListener [" + name + "] destroy found the endpoint resumed; executors kept");
                return;
            }
            if (capturedExecutor != null && capturedExecutor == lifecycleExecutor) {
                capturedExecutor.shutdown();
            }
            if (capturedCloser != null && capturedCloser == closer) {
                capturedCloser.exitWhenDetached();
            }
        }
        log.info("SolaceListener [" + name + "] destroyed (lifecycle executor shut down; closer exits after its "
                + "last close)");
    }

    void closeAll(Generation generation) {
        long startedAt = System.currentTimeMillis();
        int failures = 0;
        try {
            for (TxWorker txWorker : generation.txWorkers) {
                try {
                    txWorker.closeResources();
                } catch (Exception e) {
                    failures++;
                    log.warn("SolaceListener [" + name + "] error while closing generation " + generation.id
                            + " (transacted worker " + txWorker.index + ")", e);
                }
            }
            try {
                XMLMessageProducer parentProducer = generation.parentProducer;
                if (parentProducer != null) {
                    parentProducer.close();
                }
            } catch (Exception e) {
                failures++;
                log.warn("SolaceListener [" + name + "] error while closing generation " + generation.id
                        + " (parent producer)", e);
            }
            try {
                ConsumerBinding binding = generation.binding;
                if (binding != null) {
                    binding.close();
                }
            } catch (Exception e) {
                failures++;
                log.warn("SolaceListener [" + name + "] error while closing generation " + generation.id
                        + " (binding)", e);
            }
            try {
                XMLMessageConsumer reconnectGuard = generation.reconnectGuard;
                if (reconnectGuard != null) {
                    reconnectGuard.close();
                }
            } catch (Exception e) {
                // Quiet by A41 item 4. The session close that follows closes this consumer as well.
            }
            try {
                JCSMPSession session = generation.session;
                if (session != null && !session.isClosed()) {
                    session.closeSession();
                }
            } catch (Exception e) {
                failures++;
                log.warn("SolaceListener [" + name + "] error while closing generation " + generation.id
                        + " (session)", e);
            }
            try {
                Context context = generation.context;
                if (context != null) {
                    context.destroy();
                }
            } catch (Exception e) {
                failures++;
                log.warn("SolaceListener [" + name + "] error while closing generation " + generation.id
                        + " (context)", e);
            }
            ExecutorService workers = generation.workers;
            if (workers != null) {
                workers.shutdown();
            }
        } finally {
            generation.closer.detach(generation);
        }
        log.info("SolaceListener [" + name + "] generation " + generation.id
                + (failures == 0 ? " closed in " : " closed with " + failures + " failure(s) in ")
                + (System.currentTimeMillis() - startedAt) + " ms");
    }

    public Map<String, Object> getHealth() {
        return health.snapshot();
    }

    private void buildResources(Generation generation) throws JCSMPException {
        ContextProperties contextProperties = new ContextProperties();
        contextProperties.setName("solace-" + name + "-g" + generation.id);
        generation.context = JCSMPFactory.onlyInstance().createContext(contextProperties);
        JCSMPProperties sessionProperties = buildJCSMPProperties(generation);
        SolaceSessionEventHandler sessionHandler = new SolaceSessionEventHandler(this, generation);
        generation.sessionHandler = sessionHandler;
        generation.session = JCSMPFactory.onlyInstance().createSession(sessionProperties, generation.context,
                sessionHandler);
        generation.session.connect();
        if (StringUtils.isNotBlank(config.sharedSubscriptionGroup)
                && !generation.session.isCapable(CapabilityType.SHARED_SUBSCRIPTIONS)) {
            throw new ConfigurationFailure("solace.sharedSubscriptionGroup='" + config.sharedSubscriptionGroup
                    + "' is set but the broker does not report the SHARED_SUBSCRIPTIONS capability; remove the "
                    + "parameter or upgrade the broker");
        }
        if (!config.direct) {
            // Never started and without a subscription. It only carries the reconnect handler to JCSMP (A41).
            // A Direct endpoint registers the same handler with its own consumer in DirectTopicBinding.bind.
            generation.reconnectGuard = generation.session.getMessageConsumer(sessionHandler);
        }
        generation.workers = Executors.newFixedThreadPool(config.concurrentConsumers, new NamedThreadFactory(
                (config.transacted ? "solace-txworker-" : "solace-worker-") + name + "-" + generation.id + "-"));
        if (config.transacted) {
            generation.parentProducer = generation.session.getMessageProducer(
                    new TxPublishEventHandler(this, generation, 0));
        }
        generation.binding = config.transacted ? new TransactedBinding(this, generation)
                : ConsumerBinding.create(this, generation);
        health.bound(generation, config.transacted ? config.concurrentConsumers : 1, config.direct);
        if (config.transacted) {
            // There is no dispatcher in transacted mode. Each worker receives synchronously on its own flow (2.2).
            generation.binding.bind(null, null);
        } else {
            generation.binding.bind(new Dispatcher(this, generation),
                    new SolaceFlowEventHandler(this, generation, 1));
        }
    }

    private JCSMPProperties buildJCSMPProperties(Generation generation) {
        JCSMPProperties props = new JCSMPProperties();

        // Core connection
        props.setProperty(JCSMPProperties.HOST, config.host);
        props.setProperty(JCSMPProperties.VPN_NAME, config.vpnName);
        props.setProperty(JCSMPProperties.USERNAME, config.username);
        props.setProperty(JCSMPProperties.PASSWORD, config.password);

        String clientName = ClientNames.inbound(config.clientName, nodeId, name, config.clientNameStrict);
        if (clientName != null) {
            props.setProperty(JCSMPProperties.CLIENT_NAME, clientName);
        }

        // Authentication scheme
        applyAuthScheme(props);
        props.setBooleanProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);

        // Connection retry
        JCSMPChannelProperties channelProps = (JCSMPChannelProperties)
                props.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        channelProps.setConnectTimeoutInMillis(config.connectionTimeoutMillis);
        channelProps.setConnectRetries(config.connectRetries);
        channelProps.setConnectRetriesPerHost(config.connectRetriesPerHost);
        channelProps.setReconnectRetries(config.reconnectRetries);
        channelProps.setReconnectRetryWaitInMillis(config.reconnectRetryWaitMillis);
        if (config.compressionLevel > 0) {
            channelProps.setCompressionLevel(config.compressionLevel);
        }
        if (config.keepAliveIntervalMillis >= 0) {
            channelProps.setKeepAliveIntervalInMillis((int) config.keepAliveIntervalMillis);
        }
        if (config.keepAliveLimit >= 0) {
            channelProps.setKeepAliveLimit(config.keepAliveLimit);
        }

        // Transport window size
        props.setProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE, config.subAckWindowSize);

        props.setProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS, config.generateReceiveTimestamps);

        // SSL/TLS — custom truststore (optional; falls back to JVM default when unset)
        if (StringUtils.isNotBlank(config.sslTrustStorePath)) {
            props.setProperty(JCSMPProperties.SSL_TRUST_STORE, config.sslTrustStorePath);
            if (StringUtils.isNotBlank(config.sslTrustStorePassword)) {
                props.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD, config.sslTrustStorePassword);
            }
            if (StringUtils.isNotBlank(config.sslTrustStoreFormat)) {
                props.setProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT, config.sslTrustStoreFormat);
            }
        }

        // Only affect TLS connections.
        props.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, config.sslValidateCertificate);
        props.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE, config.sslValidateCertificateDate);
        props.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_HOST, config.sslValidateHostname);

        // Mutual TLS
        if (StringUtils.isNotBlank(config.sslKeyStorePath)) {
            props.setProperty(JCSMPProperties.SSL_KEY_STORE, config.sslKeyStorePath);
            if (StringUtils.isNotBlank(config.sslKeyStorePassword)) {
                props.setProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD, config.sslKeyStorePassword);
            }
            if (StringUtils.isNotBlank(config.sslKeyStoreFormat)) {
                props.setProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT, config.sslKeyStoreFormat);
            }
            if (StringUtils.isNotBlank(config.sslKeyPassword)) {
                props.setProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD, config.sslKeyPassword);
            }
        }

        return props;
    }

    private void applyAuthScheme(JCSMPProperties props) {
        if (SolaceInboundConstants.AUTH_SCHEME_CLIENT_CERTIFICATE
                .equalsIgnoreCase(config.authScheme)) {
            props.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                    JCSMPProperties.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE);
            // JCSMP requires a non-null USERNAME to build the SMF login frame, even when authentication is by client certificate.
            if (config.apiProvidedUsername != null && !config.apiProvidedUsername.trim().isEmpty()) {
                props.setProperty(JCSMPProperties.USERNAME, config.apiProvidedUsername);
            } else {
               // No username supplied: use a non-null placeholder so JCSMP can build the login frame.
               // Under the default broker config (username-source=common-name) the identity is derived
               // from the TLS-presented cert, so this value is not used for authentication.
               props.setProperty(JCSMPProperties.USERNAME, SolaceInboundConstants.PLACEHOLDER_USERNAME);
            }
        } else if (SolaceInboundConstants.AUTH_SCHEME_OAUTH2
                .equalsIgnoreCase(config.authScheme)) {
            props.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                    JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2);
            String accessToken = tokenSource.current();
            if (accessToken != null) {
                props.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, accessToken);
            }
            if (config.oauth2IssuerIdentifier != null) {
                props.setProperty(JCSMPProperties.OAUTH2_ISSUER_IDENTIFIER, config.oauth2IssuerIdentifier);
            }
            if (config.oidcIdToken != null) {
                props.setProperty(JCSMPProperties.OIDC_ID_TOKEN, config.oidcIdToken);
            }
            // Non-null placeholder so JCSMP can build the login frame (it NPEs on a null USERNAME).
            // Under OAuth2 the broker authenticates from the OAUTH2_ACCESS_TOKEN / OIDC_ID_TOKEN set
            // above, so this value is not used for authentication.
            props.setProperty(JCSMPProperties.USERNAME, SolaceInboundConstants.PLACEHOLDER_USERNAME);
        } else {
            props.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                    JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);
        }
    }
}
