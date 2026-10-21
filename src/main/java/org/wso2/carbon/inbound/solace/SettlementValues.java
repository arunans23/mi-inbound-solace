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

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_AUTOACK;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_DRAIN;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_INJECTION_REJECTED;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_OTHER;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_TIMEOUT;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_ACK;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_FAILED;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_REJECTED;

final class SettlementValues {

    private SettlementValues() {
    }

    static String record(String cause) {
        return cause + ":" + System.currentTimeMillis();
    }

    static String cause(String value) {
        if (value == null) {
            return CAUSE_OTHER;
        }
        if (value.equals(VALUE_ACK) || value.equals(VALUE_FAILED) || value.equals(VALUE_REJECTED)) {
            return value;
        }
        int separator = value.indexOf(':');
        String prefix = separator < 0 ? value : value.substring(0, separator);
        return (prefix.equals(CAUSE_TIMEOUT) || prefix.equals(CAUSE_DRAIN) || prefix.equals(CAUSE_INJECTION_REJECTED)
                || prefix.equals(CAUSE_AUTOACK)) ? prefix : CAUSE_OTHER;
    }

    static long ageMillis(String value) {
        int separator = value.indexOf(':');
        return separator < 0 ? -1 : System.currentTimeMillis() - Long.parseLong(value.substring(separator + 1));
    }

    static boolean isInboundRecord(String value) {
        return value != null && value.indexOf(':') > 0;
    }
}
