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
package org.apache.sling.discovery.commons.providers.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.spi.RootLogger;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.discovery.DiscoveryService;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.DefaultClusterView;
import org.apache.sling.discovery.commons.providers.DummyTopologyView;
import org.apache.sling.discovery.commons.providers.SimpleCommonsConfig;
import org.apache.sling.discovery.commons.providers.spi.ClusterSyncService;
import org.apache.sling.discovery.commons.providers.spi.base.ClusterSyncServiceChain;
import org.apache.sling.discovery.commons.providers.spi.base.DummyClusterSyncService;
import org.apache.sling.discovery.commons.providers.spi.base.DummySlingSettingsService;
import org.apache.sling.discovery.commons.providers.spi.base.IdMapService;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOakViewStateManager implements DiscoveryService {

    protected static final Logger logger = LoggerFactory.getLogger(TestOakViewStateManager.class);

    @Rule
    public final SlingContext context1 = new SlingContext();
    
    @Rule
    public final SlingContext context2 = new SlingContext();

    protected ViewStateManagerImpl mgr;

    private Level logLevel;

    @SuppressWarnings("unused")
    private IdMapService idMapService1;
    private String slingId1;

    private Scheduler scheduler;

    private TopologyView view;

    @Before
    public void setup() throws Exception {
        logger.info("setup: start");
        mgr = new ViewStateManagerImpl(new ReentrantLock(), new ClusterSyncService() {

            @Override
            public void sync(BaseTopologyView view, Runnable callback) {
                callback.run();
            }

            @Override
            public void cancelSync() {
                // nothing to cancel, we're auto-run
            }
        });
        final org.apache.log4j.Logger discoveryLogger = RootLogger.getLogger("org.apache.sling.discovery");
        logLevel = discoveryLogger.getLevel();
        discoveryLogger.setLevel(Level.INFO);
        
        slingId1 = UUID.randomUUID().toString();
        idMapService1 = IdMapService.testConstructor(new SimpleCommonsConfig(), new DummySlingSettingsService(slingId1),
                context1.getService(ResourceResolverFactory.class));
        scheduler = new DummyScheduler();
        logger.info("setup: end");
    }

    @After
    public void teardown() throws Exception {
        logger.info("teardown: start");
        if (mgr != null) {
            // release any async event sender ..
            mgr.handleDeactivated();
        }
        mgr = null;
        final org.apache.log4j.Logger discoveryLogger = RootLogger.getLogger("org.apache.sling.discovery");
        discoveryLogger.setLevel(logLevel);
        logger.info("teardown: end");
    }

    void assertEvents(DummyListener listener, TopologyEvent... events) {
        TestHelper.assertEvents(mgr, listener, events);
    }

    /**
     * This tests repetitive calls to handleNewView where a MinEventDelayHandler is
     * in play, the ClusterSyncService might be waiting 2sec in the background etc.
     * Basically load-testing the handleNewView invocation for any race conditions.
     */
    @Test
    @Ignore
    public void testRepeativeNewViewCalls() throws Exception {
        fail("do me");
    }

    /**
     * This tests the case where the ClusterSyncService is failing and then doing a
     * delay in the background while no view has previously been applied to the
     * ViewStateManager. This basically reproduces a corresponding bug (ticket tbd).
     */
    @Test
    public void testSyncServiceDelayOnFirstView_noEventDelaying() throws Exception {
        doTestSyncServiceDelayOnFirstView(false);
    }

    @Test
    public void testSyncServiceDelayOnFirstView_withEventDelaying() throws Exception {
        doTestSyncServiceDelayOnFirstView(true);
    }

    private void doTestSyncServiceDelayOnFirstView(boolean minEventDelayHandler) throws InterruptedException {
        final DummyListener listener = new DummyListener();

        final String slingId1 = UUID.randomUUID().toString();
        final String slingId2 = UUID.randomUUID().toString();
        final String slingId3 = UUID.randomUUID().toString();
        final String clusterId = UUID.randomUUID().toString();
        final DummyTopologyView view0 = new DummyTopologyView().addInstance(slingId1, new DefaultClusterView(clusterId), true, true);
        final DefaultClusterView cluster = new DefaultClusterView(clusterId);
        final DummyTopologyView view1 = new DummyTopologyView().addInstance(slingId1, cluster, true, true)
                .addInstance(slingId2, cluster, false, false).addInstance(slingId3, cluster, false, false);
//        final DummyTopologyView view2 = DummyTopologyView.clone(view1).removeInstance(slingId2);
//        final DummyTopologyView view3 = DummyTopologyView.clone(view1).removeInstance(slingId2).removeInstance(slingId3);
//        DummyTopologyView view1Cloned = DummyTopologyView.clone(view1);

        final DummyClusterSyncService s1 = new DummyClusterSyncService(3600000, 10, "s1");
        final DummyClusterSyncService s2 = new DummyClusterSyncService(3600000, 10, "s2");
        final ClusterSyncServiceChain chain = new ClusterSyncServiceChain(s1, s2);

        try {
            mgr = new ViewStateManagerImpl(new ReentrantLock(), chain);
            mgr.bind(listener);
            if (minEventDelayHandler) {
                mgr.installMinEventDelayHandler(this, scheduler, 1);
            }
            logger.info("testSyncServiceDelayOnFirstView: start");
            mgr.handleActivated();
            s1.setCheckResult(true);
            s2.setCheckResult(true);
            this.view = view0;
            mgr.handleNewView(view0);
            assertTrue(waitForCondition(new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    return listener.countEvents() == 1;
                }

            }, 5000));
            logger.info("testSyncServiceDelayOnFirstView: second call to handleNewView");
            s1.setCheckResult(false);
            s2.setCheckResult(false);
            s1.resetCounter();
            s2.resetCounter();
            s1.setCheckSemaphoreSetPermits(2);
            this.view = view1;
            mgr.handleNewView(view1);
            // waiting for at least 2 calls to check()
            // first is synchronous, second in the background
            // and we want to ensure the background thread has started
            assertTrue(s1.waitForCheckCounterAtMin(2, 5000));
            assertEquals(0, s2.getCheckCounter());

            // wait until s1 is blocked
            assertTrue(s1.waitForCheckBlockingAtMin(1, 5000));

            // now let the frist condition succeed (still blocked though)
            s1.setCheckResult(true);
            s2.setCheckSemaphoreSetPermits(0);

            s1.setCheckSemaphoreRelease(1);
            assertTrue(s2.waitForCheckBlockingAtMin(1, 5000));

            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    mgr.handleNewView(view1);
                }

            });
            t.start();
            t.join(5000);

            assertTrue(s1.waitForCheckBlockingAtMin(1, 5000));

            s1.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);
            assertTrue(s2.waitForCheckBlockingAtMin(2, 5000));

            // 100 for 2 threads, each looping at 10ms => 1sec max
            s2.setCheckSemaphoreRelease(100);

            Thread.sleep(2000);
            final long s1Blocking = s1.getCheckBlocking();
            final long s2Blocking = s2.getCheckBlocking();
            assertEquals(0, s1Blocking);
            assertEquals(1, s2Blocking);
        } finally {
            // reelease in case of test failures to not block tearDown()
            s1.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);
            s2.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);
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

    @Override
    public TopologyView getTopology() {
        return view;
    }
}