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

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JCSMP flow event handler of one generation. It runs on JCSMP threads, returns at once and makes
 * no JCSMP call. Events from a retired generation are ignored. A {@code FLOW_DOWN} carrying a
 * {@link JCSMPTransportException} is transient, because JCSMP reconnects the session and binds the
 * flow again. Any other {@code FLOW_DOWN} is terminal.
 */
public class SolaceFlowEventHandler implements FlowEventHandler {

    private static final Log log = LogFactory.getLog(SolaceFlowEventHandler.class);

    private final SolaceEventListener listener;
    private final Generation g;
    private final int index;

    SolaceFlowEventHandler(SolaceEventListener listener, Generation generation, int index) {
        this.listener = listener;
        this.g = generation;
        this.index = index;
    }

    @Override
    public void handleEvent(Object source, FlowEventArgs eventArgs) {
        if (g != listener.current) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceListener [" + listener.name + "] flow event from retired generation " + g.id
                        + " ignored: " + eventArgs);
            }
            return;
        }
        FlowEvent event = eventArgs.getEvent();
        if (event == FlowEvent.FLOW_UP) {
            log.info("SolaceListener [" + listener.name + "] flow UP (generation " + g.id + "): " + eventArgs);
            listener.health.flowUp(g, index);
        } else if (event == FlowEvent.FLOW_ACTIVE) {
            log.info("SolaceListener [" + listener.name + "] flow ACTIVE (now consuming on the endpoint, generation "
                    + g.id + "): " + eventArgs);
            listener.health.flowActivity(g, index, "active");
        } else if (event == FlowEvent.FLOW_INACTIVE) {
            log.info("SolaceListener [" + listener.name + "] flow INACTIVE (standby on the endpoint, generation "
                    + g.id + "): " + eventArgs);
            listener.health.flowActivity(g, index, "standby");
        } else if (event == FlowEvent.FLOW_RECONNECTING) {
            log.warn("SolaceListener [" + listener.name + "] flow reconnecting (generation " + g.id + "): "
                    + eventArgs);
            listener.health.flowReconnecting(g, index, true);
            listener.discardParked(g, "flow reconnecting");
        } else if (event == FlowEvent.FLOW_RECONNECTED) {
            log.info("SolaceListener [" + listener.name + "] flow reconnected (generation " + g.id + "): " + eventArgs);
            listener.health.flowReconnecting(g, index, false);
        } else if (event == FlowEvent.FLOW_DOWN
                && eventArgs.getException() instanceof JCSMPTransportException) {
            log.warn("SolaceListener [" + listener.name + "] flow DOWN while the session reconnects (generation "
                    + g.id + "): " + eventArgs + ". JCSMP will re-bind the flow.");
            listener.discardParked(g, "flow DOWN while the session reconnects");
        } else if (event == FlowEvent.FLOW_DOWN) {
            log.error("SolaceListener [" + listener.name + "] flow DOWN (terminal, generation " + g.id + "): "
                    + eventArgs);
            listener.terminalFailure(g, "flow DOWN", eventArgs.getException());
        } else if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listener.name + "] flow event (generation " + g.id + "): " + eventArgs);
        }
    }
}
