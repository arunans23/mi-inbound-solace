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
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

final class TxPublishEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

    private static final Log log = LogFactory.getLog(TxPublishEventHandler.class);

    private final SolaceEventListener listener;
    private final Generation g;
    private final int index;

    TxPublishEventHandler(SolaceEventListener listener, Generation generation, int index) {
        this.listener = listener;
        this.g = generation;
        this.index = index;
    }

    @Override
    public void responseReceivedEx(Object key) {
        if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listener.name + "] publish response (generation " + g.id + ", producer "
                    + index + "): " + key);
        }
    }

    @Override
    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
        if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listener.name + "] publish error (generation " + g.id + ", producer "
                    + index + "): " + key, cause);
        }
    }
}
