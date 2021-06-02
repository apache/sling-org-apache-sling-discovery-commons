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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.spi.ClusterSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows chaining of ClusterSyncService, itself implementing
 * the ClusterSyncService interface
 */
public class ClusterSyncServiceChain implements ClusterSyncService {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<ClusterSyncService> chain;

    /** counter incremented on every sync and cancel call */
    private AtomicLong syncCnt = new AtomicLong(0);

    /**
     * Creates a new chain of ClusterSyncService that calls a
     * cascaded sync with the provided ClusterSyncService.
     */
    public ClusterSyncServiceChain(ClusterSyncService... chain) {
        if (chain==null || chain.length==0) {
            throw new IllegalArgumentException("chain must be 1 or more");
        }
        this.chain = Arrays.asList(chain);
    }
    
    @Override
    public void sync(BaseTopologyView view, Runnable callback) {
        // could also use Preconditions.checkNotNull
        if (view == null) throw new NullPointerException("view must not be null");
        if (callback == null) throw new NullPointerException("callback must not be null");

        final Iterator<ClusterSyncService> chainIt = chain.iterator();
        chainedSync(view, callback, chainIt, syncCnt.getAndIncrement());
    }

    private void chainedSync(final BaseTopologyView view, final Runnable callback, 
            final Iterator<ClusterSyncService> chainIt, final long executionCnt) {
        if (!chainIt.hasNext()) {
            logger.debug("doSync: done with sync chain, invoking callback");
            callback.run();
            return;
        }
        ClusterSyncService next = chainIt.next();
        next.sync(view, new Runnable() {

            @Override
            public void run() {
                if (canExecute(executionCnt)) {
                    chainedSync(view, callback, chainIt, executionCnt);
                }
            }
            
        });
        canExecute(executionCnt);
    }

    /** SLING-10353 : checks if the execution can continue based on the provided executionCnt */
    private boolean canExecute(final long executionCnt) {
        final long currentCnt = syncCnt.get();
        if (currentCnt > executionCnt + 1) {
            // that means sync or cancel has been invoked in the meantime
            // and we might have missed it
            logger.info("canExecute : cancelling old, outdated sync ({} > {} + 1) (currentCnt > executionCnt + 1)",
                    currentCnt, executionCnt);
            cancelSync();
            return false;
        }
        return true;
    }

    @Override
    public void cancelSync() {
        // increment syncCnt to cancel any remaining old sync calls
        syncCnt.incrementAndGet();
        for (ClusterSyncService consistencyService : chain) {
            consistencyService.cancelSync();
        }
    }

}
