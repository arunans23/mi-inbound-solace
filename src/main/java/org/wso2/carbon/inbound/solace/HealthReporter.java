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

import org.wso2.carbon.inbound.solace.SolaceEventListener.State;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class HealthReporter {

    private final SolaceEventListener listener;
    private State lifecycle = State.STOPPED;
    private long lastChangedAt;
    String lastError;
    long lastErrorAt;
    int generation;
    int closeRejected;
    private final Map<String, String> connectivity = new LinkedHashMap<>();
    private final Map<String, String> activity = new LinkedHashMap<>();

    HealthReporter(SolaceEventListener listener) {
        this.listener = listener;
    }

    synchronized void lifecycle(State state) {
        lifecycle = state;
        lastChangedAt = System.currentTimeMillis();
    }

    synchronized void bound(Generation generation, int flows, boolean direct) {
        if (generation != listener.current) {
            return;
        }
        connectivity.clear();
        activity.clear();
        connectivity.put("session", "connected");
        for (int i = 1; i <= flows; i++) {
            connectivity.put("flow-" + i, "connected");
            activity.put("flow-" + i, "unknown");
        }
        if (direct) {
            connectivity.put("subscriptions", "connected");
        }
    }

    synchronized void connected(Generation generation) {
        if (generation != listener.current) {
            return;
        }
        this.generation = generation.id;
    }

    synchronized void sessionReconnecting(Generation generation, boolean on) {
        if (generation != listener.current) {
            return;
        }
        connectivity.put("session", on ? "reconnecting" : "connected");
        if (listener.config.direct) {
            connectivity.put("subscriptions", on ? "reconnecting" : "connected");
        }
    }

    synchronized void flowUp(Generation generation, int index) {
        if (generation != listener.current) {
            return;
        }
        connectivity.put("flow-" + index, "connected");
    }

    synchronized void flowReconnecting(Generation generation, int index, boolean on) {
        if (generation != listener.current) {
            return;
        }
        connectivity.put("flow-" + index, on ? "reconnecting" : "connected");
    }

    synchronized void flowActivity(Generation generation, int index, String activity) {
        if (generation != listener.current) {
            return;
        }
        this.activity.put("flow-" + index, activity);
    }

    synchronized void lastError(String text) {
        lastError = text;
        lastErrorAt = System.currentTimeMillis();
    }

    synchronized void lastError(Throwable cause) {
        lastError(cause.toString());
    }

    synchronized void closeRejected() {
        closeRejected++;
    }

    synchronized Map<String, Object> snapshot() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("lifecycle", lifecycle.name().toLowerCase(Locale.ROOT));
        values.put("connectivity", connectivityWord());
        values.put("activity", activityWord());
        values.put("status", statusWord());
        values.putAll(listener.counters.snapshot());
        values.put("lastError", lastError);
        values.put("lastErrorAt", lastErrorAt);
        values.put("generation", generation);
        values.put("closeRejected", closeRejected);
        for (Map.Entry<String, String> resource : connectivity.entrySet()) {
            values.put("connectivity." + resource.getKey(), resource.getValue());
        }
        for (Map.Entry<String, String> flow : activity.entrySet()) {
            values.put("activity." + flow.getKey(), flow.getValue());
        }
        return values;
    }

    synchronized String connectivityWord() {
        if (connectivity.isEmpty()) {
            // No resource is known before the first bind of a generation and after a refusal at listen() (A50).
            return "disconnected";
        }
        for (String resourceConnectivity : connectivity.values()) {
            if (!"connected".equals(resourceConnectivity)) {
                return "reconnecting";
            }
        }
        return "connected";
    }

    synchronized String activityWord() {
        if (activity.containsValue("active")) {
            return "active";
        }
        if (activity.isEmpty()) {
            return "unknown";
        }
        for (String activityOfFlow : activity.values()) {
            if (!"standby".equals(activityOfFlow)) {
                return "unknown";
            }
        }
        return "standby";
    }

    synchronized String statusWord() {
        if (lifecycle == State.FAILED) {
            return "failed";
        }
        if (lifecycle == State.STOPPED) {
            return "stopped";
        }
        if (lifecycle == State.CONNECTING || lifecycle == State.DRAINING || lifecycle == State.RECOVERING
                || "reconnecting".equals(connectivityWord())) {
            return "recovering";
        }
        String currentActivity = activityWord();
        if ("active".equals(currentActivity)) {
            return "active";
        }
        if ("standby".equals(currentActivity)) {
            return "standby";
        }
        return "connected";
    }
}
