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
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.solace.hooks.Hooks;

import java.util.concurrent.RejectedExecutionException;

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.DIRECT_PENDING_LIMIT;

final class Dispatcher implements XMLMessageListener {

    private static final Log log = LogFactory.getLog(Dispatcher.class);

    private final SolaceEventListener listener;
    private final Generation g;
    private final SolaceInboundConfig config;
    private final Counters counters;
    private final boolean direct;
    private final int high;
    private final int low;
    private volatile long lastDropLogAt = 0;

    Dispatcher(SolaceEventListener listener, Generation generation) {
        this.listener = listener;
        this.g = generation;
        this.config = listener.config;
        this.counters = listener.counters;
        this.direct = config.direct;
        this.high = config.highWaterMark;
        this.low = config.lowWaterMark;
    }

    @Override
    public void onReceive(BytesXMLMessage message) {
        final long receivedNanos = System.nanoTime();
        counters.received.increment();
        synchronized (g.admission) {
            if (g.retired || g.draining || !g.active) {
                return;
            }
            if (g.freePermits > 0 && g.pending.isEmpty()) {
                g.freePermits--;
                g.inFlight++;
                counters.gauge(g).inFlight.set(g.inFlight);
                counters.gauge(g).freePermits.set(g.freePermits);
                Generation.Delivery delivery = new Generation.Delivery(message, receivedNanos);
                g.inFlightDeliveries.add(delivery);
                try {
                    g.workers.execute(() -> DeliveryProcessor.process(listener, g, delivery));
                } catch (RejectedExecutionException e) {
                    g.inFlightDeliveries.remove(delivery);
                    g.freePermits++;
                    g.inFlight--;
                    counters.gauge(g).inFlight.set(g.inFlight);
                    counters.gauge(g).freePermits.set(g.freePermits);
                    counters.rejectedSubmit.increment();
                    log.warn("SolaceListener [" + listener.name + "] message " + message.getMessageId()
                            + " not submitted: generation " + g.id + " workers are shut down; left to broker "
                            + "redelivery");
                }
                return;
            }
            if (direct && g.pending.size() >= DIRECT_PENDING_LIMIT) {
                counters.dropped.increment();
                long now = System.currentTimeMillis();
                if (now - lastDropLogAt >= 60000) {
                    lastDropLogAt = now;
                    log.warn("SolaceListener [" + listener.name + "] Direct pending limit " + DIRECT_PENDING_LIMIT
                            + " reached; dropping (dropped so far " + counters.dropped.sum() + ")");
                }
                return;
            }
            g.pending.addLast(new Generation.Parked(message, receivedNanos));
            Hooks.get().point("dispatcher.afterPark", g, message);
            int size = g.pending.size();
            counters.gauge(g).pending.set(size);
            if (size > counters.gauge(g).pendingMax.get()) {
                counters.gauge(g).pendingMax.set(size);
            }
            if (!direct && !g.stopped && size >= high) {
                g.stopped = true;
                g.binding.stop();
                log.info("SolaceListener [" + listener.name + "] flow stopped at the high-water mark, pending "
                        + size);
            }
        }
    }

    @Override
    public void onException(JCSMPException exception) {
        log.error("SolaceListener [" + listener.name + "] consumer error on generation " + g.id, exception);
        listener.terminalFailure(g, "consumer exception", exception);
    }
}
