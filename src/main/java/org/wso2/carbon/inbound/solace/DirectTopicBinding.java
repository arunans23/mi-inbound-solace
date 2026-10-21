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

import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

import org.apache.commons.lang.StringUtils;
import org.apache.synapse.SynapseException;

import java.util.ArrayList;
import java.util.List;

import static org.wso2.carbon.inbound.solace.SolaceUtils.validateRequiredParam;

final class DirectTopicBinding extends ConsumerBinding {

    private XMLMessageConsumer consumer;
    private final List<String> topics = new ArrayList<>();

    DirectTopicBinding(SolaceEventListener listener, Generation generation) {
        super(listener, generation);
    }

    @Override
    void bind(XMLMessageListener dispatcher, FlowEventHandler flowEvents) throws JCSMPException {
        validateRequiredParam(config.destinationName, SolaceInboundConstants.DESTINATION_NAME,
                SolaceInboundConstants.DESTINATION_TYPE_TOPIC);

        for (String topicStr : config.destinationName.split(",")) {
            String trimmed = topicStr.trim();
            if (!trimmed.isEmpty()) {
                topics.add(trimmed);
            }
        }
        if (topics.isEmpty()) {
            throw new SynapseException("Parameter '" + SolaceInboundConstants.DESTINATION_NAME
                    + "' for " + SolaceInboundConstants.DESTINATION_TYPE_TOPIC
                    + " must contain at least one non-empty topic.");
        }

        consumer = g.session.getMessageConsumer(g.sessionHandler, dispatcher);

        for (String topic : topics) {
            String subscription = StringUtils.isNotBlank(config.sharedSubscriptionGroup)
                    ? "#share/" + config.sharedSubscriptionGroup + "/" + topic : topic;
            g.session.addSubscription(JCSMPFactory.onlyInstance().createTopic(subscription));
            log.info("SolaceListener [" + listener.name + "] subscribed to topic: " + subscription);
        }
        if (!config.coordination) {
            log.info("SolaceListener [" + listener.name + "] coordination=false on Direct topics: "
                    + (StringUtils.isNotBlank(config.sharedSubscriptionGroup)
                    ? "shared subscription group '" + config.sharedSubscriptionGroup
                            + "' spreads the stream across the nodes"
                    : "every node receives every message (set solace.sharedSubscriptionGroup to split the stream)"));
        }
    }

    @Override
    void start() throws JCSMPException {
        consumer.start();
    }

    @Override
    void stop() {
        consumer.stop();
    }

    @Override
    void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    boolean isDirect() {
        return true;
    }

    @Override
    String describe() {
        return "direct topics " + topics;
    }
}
