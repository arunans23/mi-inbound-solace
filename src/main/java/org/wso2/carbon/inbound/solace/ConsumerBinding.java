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

import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageListener;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.wso2.carbon.inbound.solace.hooks.Hooks;

import java.util.ArrayList;
import java.util.List;

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.REQUIRE_OUTCOMES_TRUE;

abstract class ConsumerBinding {

    static final Log log = LogFactory.getLog(ConsumerBinding.class);

    protected final SolaceEventListener listener;
    protected final Generation g;
    protected final SolaceInboundConfig config;

    ConsumerBinding(SolaceEventListener listener, Generation generation) {
        this.listener = listener;
        this.g = generation;
        this.config = listener.config;
    }

    abstract void bind(XMLMessageListener dispatcher, FlowEventHandler flowEvents) throws JCSMPException;

    abstract void start() throws JCSMPException;

    abstract void stop();

    abstract void close();

    abstract boolean isDirect();

    abstract String describe();

    static ConsumerBinding create(SolaceEventListener listener, Generation generation) {
        String destinationType = listener.config.destinationType;
        if (SolaceInboundConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)) {
            return new QueueFlowBinding(listener, generation);
        }
        if (SolaceInboundConstants.DESTINATION_TYPE_TOPIC.equalsIgnoreCase(destinationType)) {
            return SolaceInboundConstants.SUBSCRIPTION_TYPE_DURABLE.equalsIgnoreCase(listener.config.subscriptionType)
                    ? new DteFlowBinding(listener, generation) : new DirectTopicBinding(listener, generation);
        }
        throw new SynapseException("Unsupported destinationType: '" + destinationType
                + "'. Valid values: QUEUE, TOPIC.");
    }

    static ConsumerFlowProperties flowProperties(Endpoint endpoint, Generation generation, SolaceInboundConfig config,
                                                 Counters counters, Log log, String name) throws JCSMPException {
        ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
        flowProperties.setEndpoint(endpoint);
        flowProperties.setStartState(false);
        flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        if (generation.session.isCapable(CapabilityType.ACTIVE_FLOW_INDICATION)) {
            flowProperties.setActiveFlowIndication(true);
        } else {
            log.warn("SolaceListener [" + name + "] the broker does not report ACTIVE_FLOW_INDICATION; the flow "
                    + "binds without it and activity is reported as unknown on every node");
        }
        if (StringUtils.isNotBlank(config.selector)) {
            flowProperties.setSelector(config.selector.trim());
        }
        boolean failed = false;
        boolean rejected = false;
        Object capabilities = generation.session.getCapability(
                CapabilityType.SUPPORTED_MESSAGE_SETTLEMENT_CAPABILITIES);
        Object override = Hooks.get().point("binding.capabilities", capabilities);
        if (override instanceof List) {
            capabilities = override;
        }
        if (capabilities instanceof List && ((List<?>) capabilities).size() >= 3) {
            failed = Boolean.TRUE.equals(((List<?>) capabilities).get(1));
            rejected = Boolean.TRUE.equals(((List<?>) capabilities).get(2));
        }
        if (!config.autoAck) {
            List<String> missing = new ArrayList<>();
            if (!failed) {
                missing.add("FAILED");
            }
            if (!rejected) {
                missing.add("REJECTED");
            }
            if (!missing.isEmpty() && REQUIRE_OUTCOMES_TRUE.equals(config.requireSettlementOutcomes)) {
                throw new SynapseException("SolaceListener [" + name + "] the broker does not report support for the "
                        + "settlement outcome(s) " + missing + " that solace.requireSettlementOutcomes=TRUE requires; "
                        + "upgrade the broker or set solace.requireSettlementOutcomes=AUTO");
            }
            List<XMLMessage.Outcome> required = new ArrayList<>();
            if (failed) {
                required.add(XMLMessage.Outcome.FAILED);
            }
            if (rejected) {
                required.add(XMLMessage.Outcome.REJECTED);
            }
            if (!required.isEmpty()) {
                flowProperties.addRequiredSettlementOutcomes(required.toArray(new XMLMessage.Outcome[0]));
            }
            if (!missing.isEmpty()) {
                log.warn("SolaceListener [" + name + "] the broker reports no support for settlement outcome(s) "
                        + missing + "; a message that needs one is retained and the generation recovers "
                        + "(solace.requireSettlementOutcomes=AUTO)");
            }
        }
        Hooks.get().point("binding.flowProperties", flowProperties);
        generation.failedSupported = failed;
        generation.rejectedSupported = rejected;
        counters.gauge(generation).degradedOutcomes.set(config.autoAck ? 0 : (failed ? 0 : 1) + (rejected ? 0 : 1));
        return flowProperties;
    }

    static String outcomes(Generation generation) {
        if (generation.failedSupported && generation.rejectedSupported) {
            return ", outcomes FAILED,REJECTED";
        }
        if (generation.failedSupported) {
            return ", outcomes FAILED";
        }
        if (generation.rejectedSupported) {
            return ", outcomes REJECTED";
        }
        return ", outcomes none (degraded)";
    }

    /**
     * Builds EndpointProperties for queue/DTE provisioning from user config.
     */
    static EndpointProperties buildEndpointProperties(SolaceInboundConfig config) {
        EndpointProperties endpointProps = new EndpointProperties();
        if (SolaceInboundConstants.QUEUE_ACCESS_NON_EXCLUSIVE.equalsIgnoreCase(config.queueAccessType)) {
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        } else {
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        }
        if ("CONSUME".equalsIgnoreCase(config.queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
        } else if ("DELETE".equalsIgnoreCase(config.queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_DELETE);
        } else if ("MODIFY_TOPIC".equalsIgnoreCase(config.queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_MODIFY_TOPIC);
        } else if ("READ_ONLY".equalsIgnoreCase(config.queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_READ_ONLY);
        }
        if (config.queueQuotaMB > 0) {
            endpointProps.setQuota(config.queueQuotaMB);
        }
        if (config.queueMaxMsgSize > 0) {
            endpointProps.setMaxMsgSize(config.queueMaxMsgSize);
        }
        endpointProps.setRespectsMsgTTL(config.queueRespectTTL);
        if (config.queueMaxMsgRedelivery > 0) {
            endpointProps.setMaxMsgRedelivery(config.queueMaxMsgRedelivery);
        }
        return endpointProps;
    }
}
