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
import org.apache.synapse.Mediator;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.xml.SwitchCase;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.AbstractListMediator;
import org.apache.synapse.mediators.ListMediator;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.mediators.eip.aggregator.AggregateMediator;
import org.apache.synapse.mediators.eip.splitter.CloneMediator;
import org.apache.synapse.mediators.eip.splitter.IterateMediator;
import org.apache.synapse.mediators.filters.FilterMediator;
import org.apache.synapse.mediators.filters.SwitchMediator;
import org.apache.synapse.mediators.template.InvokeMediator;
import org.apache.synapse.mediators.v2.ForEachMediatorV2;
import org.apache.synapse.mediators.v2.ScatterGather;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class SequenceWalker {

    private static final Log log = LogFactory.getLog(SequenceWalker.class);

    private SequenceWalker() {
    }

    /**
     * NTask thread, called by listen() when autoAck=false. The check is advisory and never throws.
     */
    static void warnIfNoSettlementOperation(SynapseEnvironment environment, String name, String injectingSequence,
                                            String onErrorSequence, long settlementTimeoutMillis) {
        try {
            SynapseConfiguration configuration = environment.getSynapseConfiguration();
            Set<String> visited = new HashSet<>();
            if (walkKey(configuration, injectingSequence, visited)
                    || walkKey(configuration, onErrorSequence, visited)) {
                return;
            }
            log.warn("SolaceListener [" + name + "] autoAck=false but no solace.acknowledgeMessage, "
                    + "solace.nackMessage, solace.commit or solace.rollback was found in sequence '" + injectingSequence
                    + "', its fault sequence or the sequences they reference; every message will be settled by "
                    + "solace.settlementTimeout (" + settlementTimeoutMillis + " ms) unless a settlement arrives "
                    + "through a clone, a template or a dynamic key");
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceListener [" + name + "] settlement check skipped", e);
            }
        }
    }

    private static boolean walkKey(SynapseConfiguration configuration, String key, Set<String> visited) {
        if (key == null || key.isEmpty() || !visited.add(key)) {
            return false;
        }
        Mediator mediator = configuration.getSequence(key);
        return mediator != null && walk(configuration, mediator, visited);
    }

    private static boolean walk(SynapseConfiguration configuration, Mediator mediator, Set<String> visited) {
        if (mediator instanceof InvokeMediator) {
            if (isSettlementOperation(((InvokeMediator) mediator).getTargetTemplate())) {
                return true;
            }
        }
        if (mediator instanceof AbstractListMediator) {
            if (walkList(configuration, ((AbstractListMediator) mediator).getList(), visited)) {
                return true;
            }
        }
        if (mediator instanceof FilterMediator) {
            ListMediator elseMediator = ((FilterMediator) mediator).getElseMediator();
            if (elseMediator != null && walkList(configuration, elseMediator.getList(), visited)) {
                return true;
            }
        }
        if (mediator instanceof SwitchMediator) {
            SwitchMediator switchMediator = (SwitchMediator) mediator;
            for (SwitchCase switchCase : switchMediator.getCases()) {
                if (walk(configuration, switchCase.getCaseMediator(), visited)) {
                    return true;
                }
            }
            SwitchCase defaultCase = switchMediator.getDefaultCase();
            if (defaultCase != null && walk(configuration, defaultCase.getCaseMediator(), visited)) {
                return true;
            }
        }
        if (mediator instanceof CloneMediator) {
            if (walkTargets(configuration, ((CloneMediator) mediator).getTargets(), visited)) {
                return true;
            }
        }
        if (mediator instanceof ScatterGather) {
            if (walkTargets(configuration, ((ScatterGather) mediator).getTargets(), visited)) {
                return true;
            }
        }
        if (mediator instanceof IterateMediator) {
            if (walkTarget(configuration, ((IterateMediator) mediator).getTarget(), visited)) {
                return true;
            }
        }
        if (mediator instanceof ForEachMediatorV2) {
            if (walkTarget(configuration, ((ForEachMediatorV2) mediator).getTarget(), visited)) {
                return true;
            }
        }
        if (mediator instanceof AggregateMediator) {
            AggregateMediator aggregate = (AggregateMediator) mediator;
            if (aggregate.getOnCompleteSequence() != null) {
                if (walk(configuration, aggregate.getOnCompleteSequence(), visited)) {
                    return true;
                }
            } else if (walkKey(configuration, aggregate.getOnCompleteSequenceRef(), visited)) {
                return true;
            }
        }
        if (mediator instanceof SequenceMediator) {
            Value key = ((SequenceMediator) mediator).getKey();
            if (key != null && key.getKeyValue() != null && walkKey(configuration, key.getKeyValue(), visited)) {
                return true;
            }
        }
        return false;
    }

    private static boolean walkList(SynapseConfiguration configuration, List<Mediator> list, Set<String> visited) {
        if (list == null) {
            return false;
        }
        for (Mediator child : list) {
            if (walk(configuration, child, visited)) {
                return true;
            }
        }
        return false;
    }

    private static boolean walkTargets(SynapseConfiguration configuration, List<Target> targets, Set<String> visited) {
        if (targets == null) {
            return false;
        }
        for (Target target : targets) {
            if (walkTarget(configuration, target, visited)) {
                return true;
            }
        }
        return false;
    }

    private static boolean walkTarget(SynapseConfiguration configuration, Target target, Set<String> visited) {
        if (target == null) {
            return false;
        }
        if (target.getSequence() != null) {
            return walk(configuration, target.getSequence(), visited);
        }
        return walkKey(configuration, target.getSequenceRef(), visited);
    }

    private static boolean isSettlementOperation(String target) {
        if (target == null) {
            return false;
        }
        int dot = target.lastIndexOf('.');
        if (dot < 0) {
            return false;
        }
        String library = target.substring(0, dot);
        String operation = target.substring(dot + 1);
        return library.endsWith("solace") && (operation.equals("acknowledgeMessage")
                || operation.equals("nackMessage") || operation.equals("commit") || operation.equals("rollback"));
    }
}
