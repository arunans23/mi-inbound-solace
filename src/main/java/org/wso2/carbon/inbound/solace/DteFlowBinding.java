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

import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DurableTopicEndpoint;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageListener;

import static org.wso2.carbon.inbound.solace.SolaceUtils.validateRequiredParam;

final class DteFlowBinding extends ConsumerBinding {

    private FlowReceiver flowReceiver;

    DteFlowBinding(SolaceEventListener listener, Generation generation) {
        super(listener, generation);
    }

    @Override
    void bind(XMLMessageListener dispatcher, FlowEventHandler flowEvents) throws JCSMPException {
        String context = SolaceInboundConstants.DESTINATION_TYPE_TOPIC
                + " + " + SolaceInboundConstants.SUBSCRIPTION_TYPE_DURABLE;
        validateRequiredParam(config.destinationName, SolaceInboundConstants.DESTINATION_NAME, context);
        validateRequiredParam(config.dteName, SolaceInboundConstants.DTE_NAME, context);

        DurableTopicEndpoint dte = (DurableTopicEndpoint) JCSMPFactory.onlyInstance()
                .createDurableTopicEndpointEx(config.dteName);

        if (config.provisionDestination) {
            g.session.provision(dte, buildEndpointProperties(config),
                    JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        }

        ConsumerFlowProperties flowProperties = flowProperties(dte, g, config, listener.counters, log, listener.name);
        flowProperties.setNewSubscription(
                JCSMPFactory.onlyInstance().createTopic(config.destinationName));
        flowReceiver = g.session.createFlow(dispatcher, flowProperties, null, flowEvents);
    }

    @Override
    void start() throws JCSMPException {
        flowReceiver.start();
    }

    @Override
    void stop() {
        flowReceiver.stop();
    }

    @Override
    void close() {
        if (flowReceiver != null) {
            flowReceiver.close();
        }
    }

    @Override
    boolean isDirect() {
        return false;
    }

    @Override
    String describe() {
        return "DTE " + config.dteName + " subscription " + config.destinationName + outcomes(g);
    }
}
