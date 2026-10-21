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

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The command queue of one delivered message (design 7.9). The worker creates it before the inject
 * and places it on the context as an {@link Executor}. A connector operation submits its JCSMP work
 * here and waits for the result. The owner thread runs every command through its own runCommand,
 * inline when the caller is the owner itself.
 */
final class CommandQueue implements Executor {

    static final Object WAKE = new Object();

    private final LinkedBlockingQueue<Object> q = new LinkedBlockingQueue<>();
    private final Thread owner;
    private final Consumer<Object> ownerRunCommand;
    private volatile boolean closed = false;

    CommandQueue(Thread owner, Consumer<Object> ownerRunCommand) {
        this.owner = owner;
        this.ownerRunCommand = ownerRunCommand;
    }

    /** Any thread: an HTTP worker inside the sequence, or the owner itself. */
    @Override
    public void execute(Runnable command) {
        if (Thread.currentThread() == owner) {
            // The synchronous leg runs inline, through the same admission and execution contract as a queued command.
            if (closed) {
                throw new RejectedExecutionException("TRANSACTION_COMPLETED");
            }
            ownerRunCommand.accept(command);
            return;
        }
        synchronized (q) {
            // close() takes the same monitor. A command is either queued before the close and cancelled by it, or refused.
            if (closed) {
                throw new RejectedExecutionException("TRANSACTION_COMPLETED");
            }
            q.offer(command);
        }
    }

    void wake() {
        q.offer(WAKE);
    }

    /** A zero or negative wait still returns an available item (Java contract, design 7.9). */
    Object poll(long millis) throws InterruptedException {
        return q.poll(Math.max(0, millis), TimeUnit.MILLISECONDS);
    }

    /** Owner thread only, before commit or rollback. Every queued command completes with TRANSACTION_COMPLETED, reason first. */
    void close() {
        synchronized (q) {
            closed = true;
            Object item;
            while ((item = q.poll()) != null) {
                if (item instanceof Future) {
                    TxWorker.reason(item, "TRANSACTION_COMPLETED");
                    ((Future<?>) item).cancel(false);
                }
            }
        }
    }
}
