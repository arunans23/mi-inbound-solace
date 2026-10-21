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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.libraries.model.Library;

import java.util.Map;

final class VersionGuard {

    private static final Log log = LogFactory.getLog(VersionGuard.class);

    private VersionGuard() {
    }

    /**
     * NTask thread. Returns null when the endpoint may start, otherwise the error text.
     */
    static String check(SynapseEnvironment environment, String name) {
        try {
            Map<String, Library> libraries = environment.getSynapseConfiguration().getSynapseLibraries();
            Library solace = null;
            for (Map.Entry<String, Library> entry : libraries.entrySet()) {
                if (entry.getKey().endsWith("}solace") || "solace".equals(entry.getKey())) {
                    solace = entry.getValue();
                    break;
                }
            }
            if (solace == null) {
                return null;
            }
            ClassLoader libraryLoader = solace.getLibClassLoader();
            if (libraryLoader == null) {
                log.warn("SolaceListener [" + name + "] connector version check skipped: library "
                        + solace.getFileName() + " is deployed but not loaded (status disabled)");
                return null;
            }
            try {
                Class.forName("org.wso2.integration.connector.ConnectorVersionMarker", false, libraryLoader);
                return null;
            } catch (ClassNotFoundException e) {
                return "a Solace connector older than 2.0.0 is deployed (" + solace.getFileName()
                        + "); inbound 2.0.0 requires connector 2.0.0 (deploy it and redeploy this inbound endpoint)";
            }
        } catch (Throwable t) {
            log.warn("SolaceListener [" + name + "] connector version check skipped: " + t.getMessage());
            return null;
        }
    }
}
