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
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;

import java.util.ArrayList;
import java.util.List;

import static org.wso2.carbon.inbound.solace.SolaceUtils.validateRequiredParam;

/**
 * The binding of a transacted queue (design 7.9). There is no dispatcher in this mode. Each worker
 * owns its transacted session, flow and producer and receives synchronously, so the dispatcher and
 * the flow handler arguments of bind() are unused. The workers create their own flow handlers with
 * their index.
 */
final class TransactedBinding extends ConsumerBinding {

    TransactedBinding(SolaceEventListener listener, Generation generation) {
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

        List<TxWorker> txWorkers = new ArrayList<>(config.concurrentConsumers);
        for (int workerIndex = 1; workerIndex <= config.concurrentConsumers; workerIndex++) {
            txWorkers.add(new TxWorker(listener, g, workerIndex));
        }
        // Stored before any open, so closeAll(g) can release a partial build.
        g.txWorkers = txWorkers;
        for (TxWorker txWorker : txWorkers) {
            txWorker.publish(txWorker.openResources());
        }
    }

    /** Lifecycle thread, inside the activation barrier. */
    @Override
    void start() throws JCSMPException {
        for (TxWorker txWorker : g.txWorkers) {
            txWorker.current.flow.start();
            g.workers.execute(txWorker);
        }
    }

    /** The drain only, under the admission lock. Non blocking, and harmless on a set a rebuild closed meanwhile. */
    @Override
    void stop() {
        for (TxWorker txWorker : g.txWorkers) {
            TxWorker.ResourceSet resources = txWorker.current;
            if (resources != null && resources.flow != null) {
                resources.flow.stop();
            }
        }
    }

    /** Closer thread only. Idempotent with the closeAll loop over the workers. */
    @Override
    void close() {
        for (TxWorker txWorker : g.txWorkers) {
            txWorker.closeResources();
        }
    }

    @Override
    boolean isDirect() {
        return false;
    }

    @Override
    String describe() {
        return "transacted queue " + config.destinationName + ", " + config.concurrentConsumers + " flow(s)";
    }
}
