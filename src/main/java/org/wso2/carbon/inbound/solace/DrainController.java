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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_DRAIN;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.SETTLE_BUDGET_MS;

final class DrainController {

    private static final Log log = LogFactory.getLog(DrainController.class);

    private DrainController() {
    }

    /**
     * Caller: pause() (NTask thread) or recover() (lifecycle thread). Returns at deadline at the latest.
     * No lifecycle lock held.
     */
    static void drain(SolaceEventListener listener, Generation generation, long deadline) {
        Counters counters = listener.counters;
        String name = listener.name;
        synchronized (generation.admission) {
            generation.draining = true;
            if (!generation.stopped) {
                generation.stopped = true;
                generation.binding.stop();
            }
            int parked = generation.pending.size();
            generation.pending.clear();
            counters.gauge(generation).pending.set(0);
            counters.leftByDrain.add(parked);
            for (Generation.SleepToken token : generation.sleepers) {
                if (token.state.compareAndSet(Generation.SleepToken.SLEEPING, Generation.SleepToken.INTERRUPTED)) {
                    token.thread.interrupt();
                }
            }
        }
        waitForInFlight(generation, deadline - SETTLE_BUDGET_MS);
        List<CompletableFuture<String>> handles;
        synchronized (generation.admission) {
            handles = new ArrayList<>(generation.inFlightHandles.keySet());
        }
        int completed = 0;
        for (CompletableFuture<String> handle : handles) {
            if (handle.complete(SettlementValues.record(CAUSE_DRAIN))) {
                completed++;
            }
        }
        if (completed > 0) {
            log.info("SolaceListener [" + name + "] drain of generation " + generation.id + ": " + completed
                    + " in-flight message(s) completed with DRAIN; applying drainOutcome="
                    + listener.config.drainOutcome + " within " + SETTLE_BUDGET_MS + " ms");
        }
        for (TxWorker txWorker : generation.txWorkers) {
            txWorker.drainRequested();
        }
        waitForInFlight(generation, deadline);
        int remaining;
        List<String> ids = new ArrayList<>();
        int newlyAbandoned = 0;
        synchronized (generation.admission) {
            remaining = generation.inFlight;
            for (Generation.Delivery delivery : generation.inFlightDeliveries) {
                if (!delivery.abandoned) {
                    delivery.abandoned = true;
                    newlyAbandoned++;
                    ids.add(delivery.messageId);
                }
            }
        }
        if (remaining > 0) {
            counters.abandoned.add(newlyAbandoned);
            log.warn("SolaceListener [" + name + "] drain of generation " + generation.id
                    + " reached its deadline with " + remaining
                    + " message(s) still in flight; they are abandoned to broker redelivery after "
                    + "unbind");
            for (String id : ids) {
                log.warn("SolaceListener [" + name + "] abandoned message " + id + " (generation " + generation.id
                        + ")");
            }
        }
    }

    private static void waitForInFlight(Generation generation, long until) {
        synchronized (generation.admission) {
            while (generation.inFlight > 0) {
                long remaining = until - System.currentTimeMillis();
                if (remaining <= 0) {
                    return;
                }
                try {
                    generation.admission.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
