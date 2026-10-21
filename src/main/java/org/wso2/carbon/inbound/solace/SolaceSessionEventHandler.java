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

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPReconnectEventHandler;
import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JCSMP session event handler of one generation. It runs on JCSMP threads, returns at once and
 * makes no JCSMP call. Events from a retired generation are ignored. It also answers the reconnect
 * question of JCSMP: a generation that is retired or closing refuses its own reconnect.
 */
public class SolaceSessionEventHandler implements SessionEventHandler, JCSMPReconnectEventHandler {

    private static final Log log = LogFactory.getLog(SolaceSessionEventHandler.class);

    private final SolaceEventListener listener;
    private final Generation g;
    private final SolaceInboundConfig config;

    SolaceSessionEventHandler(SolaceEventListener listener, Generation generation) {
        this.listener = listener;
        this.g = generation;
        this.config = listener.config;
    }

    @Override
    public void handleEvent(SessionEventArgs eventArgs) {
        if (g != listener.current) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceListener [" + listener.name + "] session event from retired generation " + g.id
                        + " ignored: " + eventArgs);
            }
            return;
        }
        SessionEvent event = eventArgs.getEvent();
        if (event == SessionEvent.RECONNECTING) {
            if (SolaceInboundConstants.AUTH_SCHEME_OAUTH2.equalsIgnoreCase(config.authScheme)
                    && config.oauth2TokenFile != null) {
                // A local property update with no broker round trip, legal on the notification executor.
                try {
                    g.session.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, listener.tokenSource.current());
                } catch (JCSMPException e) {
                    log.warn("SolaceListener [" + listener.name + "] could not update the OAuth2 token on the "
                            + "reconnecting session", e);
                }
            }
            log.warn("SolaceListener [" + listener.name + "] session reconnecting (generation " + g.id + "): "
                    + eventArgs);
            listener.health.sessionReconnecting(g, true);
        } else if (event == SessionEvent.RECONNECTED) {
            log.info("SolaceListener [" + listener.name + "] session reconnected (generation " + g.id + "): "
                    + eventArgs);
            listener.health.sessionReconnecting(g, false);
        } else if (event == SessionEvent.DOWN_ERROR) {
            log.error("SolaceListener [" + listener.name + "] session DOWN (terminal, generation " + g.id + "): "
                    + eventArgs);
            listener.terminalFailure(g, "session DOWN_ERROR", eventArgs.getException());
        } else if (event == SessionEvent.SUBSCRIPTION_ERROR) {
            if (config.direct) {
                log.error("SolaceListener [" + listener.name + "] subscription error (terminal for a Direct topic, "
                        + "generation " + g.id + "): " + eventArgs);
                listener.terminalFailure(g, "subscription error", eventArgs.getException());
            } else {
                log.warn("SolaceListener [" + listener.name + "] subscription error (generation " + g.id + "): "
                        + eventArgs);
            }
        } else if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listener.name + "] session event (generation " + g.id + "): " + eventArgs);
        }
    }

    // The flags alone decide, never listener.current, so a generation that lost at its barrier is covered.
    @Override
    public boolean preReconnect() {
        boolean allowed = !(g.retired || g.closing);
        if (allowed) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceListener [" + listener.name + "] reconnect allowed (generation " + g.id + ")");
            }
        } else {
            log.warn("SolaceListener [" + listener.name + "] reconnect refused: generation " + g.id
                    + " is retired or closing, the session stays down for the closer");
        }
        return allowed;
    }

    @Override
    public void postReconnect() {
        if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listener.name + "] reconnect completed (generation " + g.id + ")");
        }
    }
}
