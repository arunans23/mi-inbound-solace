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
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;

import static org.wso2.carbon.inbound.solace.SolaceUtils.validateRequiredParam;

final class QueueFlowBinding extends ConsumerBinding {

    private FlowReceiver flowReceiver;

    QueueFlowBinding(SolaceEventListener listener, Generation generation) {
        super(listener, generation);
    }

    @Override
    void bind(XMLMessageListener dispatcher, FlowEventHandler flowEvents) throws JCSMPException {
        validateRequiredParam(config.destinationName, SolaceInboundConstants.DESTINATION_NAME,
                SolaceInboundConstants.DESTINATION_TYPE_QUEUE);
        Queue queue = JCSMPFactory.onlyInstance().createQueue(config.destinationName);

        if (config.provisionDestination) {
            g.session.provision(queue, buildEndpointProperties(config),
                    JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        }

        ConsumerFlowProperties flowProperties = flowProperties(queue, g, config, listener.counters, log, listener.name);
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
        return "queue " + config.destinationName + outcomes(g);
    }
}
