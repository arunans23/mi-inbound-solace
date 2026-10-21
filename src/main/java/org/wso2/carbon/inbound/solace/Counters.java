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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_AUTOACK;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_DRAIN;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_INJECTION_REJECTED;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_OTHER;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_TIMEOUT;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_ACK;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_FAILED;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_REJECTED;

final class Counters {

    static final class Gauges {
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger pending = new AtomicInteger();
        final AtomicInteger pendingMax = new AtomicInteger();
        final AtomicInteger freePermits;
        final AtomicInteger degradedOutcomes = new AtomicInteger();

        Gauges(int permits) {
            freePermits = new AtomicInteger(permits);
        }
    }

    private static final String[] DECIDED_CAUSES = {VALUE_ACK, VALUE_FAILED, VALUE_REJECTED, CAUSE_TIMEOUT,
            CAUSE_DRAIN, CAUSE_INJECTION_REJECTED, CAUSE_AUTOACK, CAUSE_OTHER};

    private volatile Gauges current = new Gauges(0);

    final LongAdder received = new LongAdder();
    final LongAdder redelivered = new LongAdder();
    private final Map<String, LongAdder> decided = new LinkedHashMap<>();
    private final Map<String, LongAdder> applied = new LinkedHashMap<>();
    final LongAdder settledAck = new LongAdder();
    final LongAdder settledFailed = new LongAdder();
    final LongAdder settledRejected = new LongAdder();
    final LongAdder retried = new LongAdder();
    final LongAdder retainedUnsettled = new LongAdder();
    final LongAdder settleFailed = new LongAdder();
    final LongAdder handlerFailed = new LongAdder();
    final LongAdder rejectedSubmit = new LongAdder();
    final LongAdder leftByDrain = new LongAdder();
    final LongAdder abandoned = new LongAdder();
    final LongAdder dropped = new LongAdder();
    final LongAdder unknown = new LongAdder();
    final LongAdder committed = new LongAdder();
    final LongAdder rolledBack = new LongAdder();
    final LongAdder rolledBackByBroker = new LongAdder();

    Counters() {
        for (String cause : DECIDED_CAUSES) {
            decided.put(cause, new LongAdder());
        }
        for (DeliveryProcessor.Decision decision : DeliveryProcessor.Decision.values()) {
            applied.put(decision.name(), new LongAdder());
        }
    }

    LongAdder decided(String cause) {
        LongAdder adder = decided.get(cause);
        return adder != null ? adder : decided.get(CAUSE_OTHER);
    }

    LongAdder applied(DeliveryProcessor.Decision decision) {
        return applied.get(decision.name());
    }

    void currentGeneration(Generation generation) {
        current = generation.gauges;
    }

    Gauges gauge(Generation generation) {
        return generation.gauges;
    }

    Map<String, Object> snapshot() {
        Gauges gauges = current;
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("inFlight", gauges.inFlight.get());
        values.put("pending", gauges.pending.get());
        values.put("pendingMax", gauges.pendingMax.get());
        values.put("freePermits", gauges.freePermits.get());
        values.put("degradedOutcomes", gauges.degradedOutcomes.get());
        values.put("received", received.sum());
        values.put("redelivered", redelivered.sum());
        for (Map.Entry<String, LongAdder> entry : decided.entrySet()) {
            values.put("decided." + entry.getKey(), entry.getValue().sum());
        }
        for (Map.Entry<String, LongAdder> entry : applied.entrySet()) {
            values.put("applied." + entry.getKey(), entry.getValue().sum());
        }
        values.put("settled.ack", settledAck.sum());
        values.put("settled.failed", settledFailed.sum());
        values.put("settled.rejected", settledRejected.sum());
        values.put("retried", retried.sum());
        values.put("retainedUnsettled", retainedUnsettled.sum());
        values.put("settleFailed", settleFailed.sum());
        values.put("handlerFailed", handlerFailed.sum());
        values.put("rejectedSubmit", rejectedSubmit.sum());
        values.put("leftByDrain", leftByDrain.sum());
        values.put("abandoned", abandoned.sum());
        values.put("dropped", dropped.sum());
        values.put("unknown", unknown.sum());
        values.put("committed", committed.sum());
        values.put("rolledBack", rolledBack.sum());
        values.put("rolledBackByBroker", rolledBackByBroker.sum());
        return values;
    }
}
