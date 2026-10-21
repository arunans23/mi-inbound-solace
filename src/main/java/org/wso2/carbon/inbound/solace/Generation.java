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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageProducer;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

final class Generation {

    final int id;
    final ScheduledExecutorService lifecycleExecutor;
    final Closer closer;
    final SolaceInboundConfig config;
    final Object admission = new Object();
    int freePermits;
    int inFlight = 0;
    final ArrayDeque<Parked> pending = new ArrayDeque<>();
    boolean stopped = false;
    boolean draining = false;
    final Map<CompletableFuture<String>, Delivery> inFlightHandles = new HashMap<>();
    final Set<Delivery> inFlightDeliveries = new HashSet<>();
    final Set<SleepToken> sleepers = new HashSet<>();
    // Lifecycle fields. SolaceEventListener.lifecycleLock guards them, and the volatile ones are read without it.
    volatile boolean retired = false;
    volatile boolean active = false;
    // Set by Closer.submit before anything else, so a generation that lost at its barrier refuses its reconnect too (A41).
    volatile boolean closing = false;
    boolean claimed = false;
    ScheduledFuture<?> scheduled;
    // Resources. The builder thread writes them before the activation barrier, and any thread reads them after it.
    volatile Context context;
    volatile JCSMPSession session;
    volatile SolaceSessionEventHandler sessionHandler;
    volatile XMLMessageConsumer reconnectGuard;
    volatile ConsumerBinding binding;
    volatile ExecutorService workers;
    volatile List<TxWorker> txWorkers = Collections.emptyList();
    volatile XMLMessageProducer parentProducer;
    boolean failedSupported;
    boolean rejectedSupported;
    final Counters.Gauges gauges;

    Generation(int id, ScheduledExecutorService lifecycleExecutor, Closer closer, SolaceInboundConfig config) {
        this.id = id;
        this.lifecycleExecutor = lifecycleExecutor;
        this.closer = closer;
        this.config = config;
        this.freePermits = config.concurrentConsumers;
        this.gauges = new Counters.Gauges(config.concurrentConsumers);
        closer.attach(this);
    }

    static final class SleepToken {
        static final int SLEEPING = 1;
        static final int INTERRUPTED = 2;
        static final int DONE = 3;
        final Thread thread;
        final AtomicInteger state = new AtomicInteger(SLEEPING);

        SleepToken(Thread thread) {
            this.thread = thread;
        }
    }

    static final class Parked {
        final BytesXMLMessage msg;
        final long receivedNanos;

        Parked(BytesXMLMessage message, long receivedNanos) {
            this.msg = message;
            this.receivedNanos = receivedNanos;
        }
    }

    static final class Delivery {
        final BytesXMLMessage msg;
        final String messageId;
        final long receivedNanos;
        volatile Thread worker;
        boolean abandoned;
        long completionNanos;

        Delivery(BytesXMLMessage message, long receivedNanos) {
            this.msg = message;
            this.messageId = message.getMessageId();
            this.receivedNanos = receivedNanos;
        }
    }
}
