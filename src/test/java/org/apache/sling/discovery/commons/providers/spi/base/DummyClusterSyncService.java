/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.discovery.commons.providers.spi.base;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.spi.ClusterSyncService;

public class DummyClusterSyncService extends AbstractServiceWithBackgroundCheck implements ClusterSyncService {

    private final long timeoutMillis;
    private final long intervalMillis;
    private final String debugName;

    private final AtomicBoolean checkResult = new AtomicBoolean(false);
    private final AtomicLong checkCounter = new AtomicLong(0);
    private final Semaphore checkSemaphore = new Semaphore(Integer.MAX_VALUE);
    private final AtomicInteger checkBlocking = new AtomicInteger(0);

    public DummyClusterSyncService(long timeoutMillis, long intervalMillis, String debugName) {
        this.timeoutMillis = timeoutMillis;
        this.intervalMillis = intervalMillis;
        this.debugName = debugName;
    }

    public void setCheckResult(boolean checkResult) {
        this.checkResult.set(checkResult);
    }

    public boolean getCheckResult() {
        return checkResult.get();
    }

    public void setCheckSemaphoreSetPermits(int permits) {
        this.checkSemaphore.drainPermits();
        this.checkSemaphore.release(permits);
    }

    public void setCheckSemaphoreRelease(int permits) {
        this.checkSemaphore.release(permits);
    }

    @Override
    public void sync(BaseTopologyView view, Runnable callback) {
        startBackgroundCheck(debugName, new BackgroundCheck() {

            @Override
            public boolean check() {
                boolean incremented = false;
                try {
                    if (!checkSemaphore.tryAcquire()) {
                        checkBlocking.incrementAndGet();
                        incremented = true;
                        while (true) {
                            try {
                                checkSemaphore.acquire();
                                break;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    return checkResult.get();
                } finally {
                    if (incremented) {
                        checkBlocking.decrementAndGet();
                    }
                    checkCounter.incrementAndGet();
                }
            }
        }, callback, timeoutMillis, intervalMillis);
    }

    public boolean waitForCheckCounterAtMin(final long minValue, long timeoutMillis) {
        return waitForCondition(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return checkCounter.get() >= minValue;
            }

        }, timeoutMillis);
    }

    public boolean waitForCheckBlockingAtMin(final int minBlockedCnt, long timeoutMillis) {
        return waitForCondition(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return checkBlocking.get() >= minBlockedCnt;
            }

        }, timeoutMillis);
    }

    public boolean waitForCheckBlockingAtMax(final int maxBlockedCnt, long timeoutMillis) {
        return waitForCondition(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return checkBlocking.get() <= maxBlockedCnt;
            }

        }, timeoutMillis);
    }

    public long getCheckCounter() {
        return checkCounter.get();
    }

    public int getCheckBlocking() {
        return checkBlocking.get();
    }

    @Override
    public void cancelSync() {
        cancelPreviousBackgroundCheck();
    }

    public boolean waitForBackgroundCheckFinished(long timeoutMillis) {
        return waitForCondition(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return !hasBackgroundCheckRunnable();
            }

        }, timeoutMillis);
    }

    public boolean hasBackgroundCheckRunnable() {
        final BackgroundCheckRunnable r = backgroundCheckRunnable;
        if (r == null) {
            return false;
        } else {
            return !r.isDone();
        }
    }

    private boolean waitForCondition(Callable<Boolean> condition, long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis must be 0 or positive, is: " + timeoutMillis);
        }
        final long timeout = System.currentTimeMillis() + timeoutMillis;
        try {
            while (!condition.call()) {
                final long delta = Math.min(10, timeout - System.currentTimeMillis());
                if (delta <= 0) {
                    // timeout
                    break;
                } else {
                    try {
                        Thread.sleep(delta);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            return condition.call();
        } catch (Exception e) {
            throw new AssertionError("Got Exception: " + e, e);
        }
    }

}