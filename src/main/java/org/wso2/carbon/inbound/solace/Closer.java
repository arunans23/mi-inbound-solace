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
import org.wso2.carbon.inbound.solace.hooks.Hooks;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class Closer {

    private static final Log log = LogFactory.getLog(Closer.class);

    private final ThreadPoolExecutor executor;
    private final AtomicInteger attached = new AtomicInteger();
    private final HealthReporter health;
    private final String name;
    volatile boolean exiting = false;

    Closer(String name, HealthReporter health) {
        this.name = name;
        this.health = health;
        executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                new NamedThreadFactory("solace-closer-" + name, true));
    }

    void attach(Generation generation) {
        attached.incrementAndGet();
    }

    void detach(Generation generation) {
        Hooks.get().point("counters.detached");
        if (attached.decrementAndGet() == 0 && exiting) {
            executor.shutdown();
        }
    }

    void exitWhenDetached() {
        exiting = true;
        if (attached.get() == 0) {
            executor.shutdown();
        }
    }

    void submit(Generation generation, Runnable close) {
        generation.closing = true;
        Hooks.get().point("counters.closeSubmitted");
        try {
            executor.execute(close);
        } catch (RejectedExecutionException e) {
            log.error("SolaceListener [" + name + "] closer rejected the close of generation " + generation.id
                    + "; its resources are left open");
            health.closeRejected();
        }
    }
}
