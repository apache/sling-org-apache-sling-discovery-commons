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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.discovery.commons.providers.DummyTopologyView;
import org.apache.sling.discovery.commons.providers.SimpleCommonsConfig;
import org.apache.sling.discovery.commons.providers.ViewStateManager;
import org.apache.sling.discovery.commons.providers.base.DummyListener;
import org.apache.sling.discovery.commons.providers.base.TestHelper;
import org.apache.sling.discovery.commons.providers.base.ViewStateManagerFactory;
import org.apache.sling.discovery.commons.providers.spi.LocalClusterView;
import org.apache.sling.discovery.commons.providers.spi.base.AbstractServiceWithBackgroundCheck.BackgroundCheckRunnable;
import org.apache.sling.jcr.api.SlingRepository;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.NodeTypeMode;
import org.apache.sling.testing.mock.sling.oak.OakMockResourceResolverAdapter;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOakSyncTokenService {

    private final static Logger logger = LoggerFactory.getLogger(TestOakSyncTokenService.class);

    ResourceResolverFactory factory1;
    ResourceResolverFactory factory2;
    private IdMapService idMapService1;
    private String slingId1;
    
    @Before
    public void setup() throws Exception {
        logger.info("setup: start");
        
        BundleContext bundleContext1 = MockOsgi.newBundleContext();
        BundleContext bundleContext2 = MockOsgi.newBundleContext();
        
        // We need to create to ResourceResolverFactories that share the same repository. This is not supported
        // with the current sling jcr mocks so ... we improvise
        
        // 1. create two SlingRepository instances and make sure that they share the same nodeStore
        OakMockResourceResolverAdapter oakAdapter = new OakMockResourceResolverAdapter();
        SlingRepository initialRepo = oakAdapter.newSlingRepository();
        SlingRepository secondRepo = oakAdapter.newSlingRepository();
        // use reflection to copy the nodeStore from initialRepo to secondRepo
        Field field = initialRepo.getClass().getDeclaredField("nodeStore");
        field.setAccessible(true);
        Object nodeStore = field.get(initialRepo);
        field = secondRepo.getClass().getDeclaredField("nodeStore");
        field.setAccessible(true);
        field.set(secondRepo, nodeStore);
        

        // 2. create the two ResourceResolverFactories from existing SlingRepository instances by using
        // the internal ResourceResolverFactoryInitializer class
        Class<?> initialiserClass = getClass().getClassLoader().loadClass("org.apache.sling.testing.mock.sling.ResourceResolverFactoryInitializer");
        Method setupMethod = initialiserClass.getMethod("setUp", SlingRepository.class, BundleContext.class, NodeTypeMode.class);
        setupMethod.setAccessible(true);
        factory1 = (ResourceResolverFactory) setupMethod.invoke(null,
                initialRepo, bundleContext1, NodeTypeMode.NODETYPES_REQUIRED);

        factory2 = (ResourceResolverFactory) setupMethod.invoke(null,
                secondRepo, bundleContext2, NodeTypeMode.NODETYPES_REQUIRED);
        
        slingId1 = UUID.randomUUID().toString();
        idMapService1 = IdMapService.testConstructor(new SimpleCommonsConfig(), new DummySlingSettingsService(slingId1), factory1);
        logger.info("setup: end");
    }
    
    @Test
    public void testOneNode() throws Exception {
        logger.info("testOneNode: start");
        DummyTopologyView one = TestHelper.newView(true, slingId1, slingId1, slingId1);
        Lock lock = new ReentrantLock();
        OakBacklogClusterSyncService cs = OakBacklogClusterSyncService.testConstructorAndActivate(new SimpleCommonsConfig(), idMapService1, new DummySlingSettingsService(slingId1), factory1);
        ViewStateManager vsm = ViewStateManagerFactory.newViewStateManager(lock, cs);
        DummyListener l = new DummyListener();
        assertEquals(0, l.countEvents());
        vsm.bind(l);
        cs.triggerBackgroundCheck();
        assertEquals(0, l.countEvents());
        vsm.handleActivated();
        cs.triggerBackgroundCheck();
        assertEquals(0, l.countEvents());
        vsm.handleNewView(one);
        cs.triggerBackgroundCheck();
        assertEquals(0, l.countEvents());
        cs.triggerBackgroundCheck();
        DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().me(1).seq(1).activeIds(1).setFinal(true));
        assertTrue(idMapService1.waitForInit(5000));
        cs.triggerBackgroundCheck();
        assertEquals(0, vsm.waitForAsyncEvents(1000));
        assertEquals(1, l.countEvents());
        logger.info("testOneNode: end");
    }
    
    @Test
    public void testTwoNodesOneLeaving() throws Exception {
        logger.info("testTwoNodesOneLeaving: start");
        String slingId2 = UUID.randomUUID().toString();
        DummyTopologyView two1 = TestHelper.newView(true, slingId1, slingId1, slingId1, slingId2);
        Lock lock1 = new ReentrantLock();
        OakBacklogClusterSyncService cs1 = OakBacklogClusterSyncService.testConstructorAndActivate(new SimpleCommonsConfig(), idMapService1, new DummySlingSettingsService(slingId1), factory1);
        ViewStateManager vsm1 = ViewStateManagerFactory.newViewStateManager(lock1, cs1);
        DummyListener l = new DummyListener();
        vsm1.bind(l);
        vsm1.handleActivated();
        vsm1.handleNewView(two1);
        cs1.triggerBackgroundCheck();
        assertEquals(0, l.countEvents());
        DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().setFinal(true).me(1).seq(1).activeIds(1).deactivatingIds(2));
        cs1.triggerBackgroundCheck();
        assertEquals(0, l.countEvents());
        
        // make an assertion that the background runnable is at this stage - even with
        // a 2sec sleep - waiting for the deactivating instance to disappear
        logger.info("testTwoNodesOneLeaving: sync service should be waiting for backlog to disappear");
        Thread.sleep(2000);
        BackgroundCheckRunnable backgroundCheckRunnable = cs1.backgroundCheckRunnable;
        assertNotNull(backgroundCheckRunnable);
        assertFalse(backgroundCheckRunnable.isDone());
        assertFalse(backgroundCheckRunnable.cancelled());
        
        // release the deactivating instance by removing it from the clusterView
        logger.info("testTwoNodesOneLeaving: freeing backlog - sync service should finish up");
        DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().setFinal(true).me(1).seq(2).activeIds(1));
        cs1.triggerBackgroundCheck();
        
        // now give this thing 2 sec to settle
        Thread.sleep(2000);
        
        // after that, the backgroundRunnable should be done and no events stuck in vsm
        backgroundCheckRunnable = cs1.backgroundCheckRunnable;
        assertNotNull(backgroundCheckRunnable);
        assertFalse(backgroundCheckRunnable.cancelled());
        assertTrue(backgroundCheckRunnable.isDone());
        assertEquals(0, vsm1.waitForAsyncEvents(1000));
        
        logger.info("testTwoNodesOneLeaving: setting up 2nd node");
        Lock lock2 = new ReentrantLock();
        IdMapService idMapService2 = IdMapService.testConstructor(
                new SimpleCommonsConfig(), new DummySlingSettingsService(slingId2), factory2);
        OakBacklogClusterSyncService cs2 = OakBacklogClusterSyncService.testConstructorAndActivate(new SimpleCommonsConfig(), idMapService2, new DummySlingSettingsService(slingId2), factory2);
        ViewStateManager vsm2 = ViewStateManagerFactory.newViewStateManager(lock2, cs2);
        cs1.triggerBackgroundCheck();
        cs2.triggerBackgroundCheck();
        assertEquals(1, l.countEvents());
        DescriptorHelper.setDiscoveryLiteDescriptor(factory2, new DiscoveryLiteDescriptorBuilder().setFinal(true).me(2).seq(3).activeIds(1, 2));
        cs1.triggerBackgroundCheck();
        cs2.triggerBackgroundCheck();
        assertEquals(1, l.countEvents());
        DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().setFinal(true).me(1).seq(3).activeIds(1, 2));
        cs1.triggerBackgroundCheck();
        cs2.triggerBackgroundCheck();
        assertEquals(1, l.countEvents());
        vsm2.handleActivated();
        assertTrue(idMapService1.waitForInit(5000));
        assertTrue(idMapService2.waitForInit(5000));
        DummyTopologyView two2 = TestHelper.newView(two1.getLocalClusterSyncTokenId(), two1.getLocalInstance().getClusterView().getId(), true, slingId1, slingId1, slingId1, slingId2);
        vsm2.handleNewView(two2);
        cs1.triggerBackgroundCheck();
        cs1.triggerBackgroundCheck();
        cs2.triggerBackgroundCheck();
        cs2.triggerBackgroundCheck();
        assertEquals(0, vsm1.waitForAsyncEvents(1000));
        assertEquals(1, l.countEvents());
        
        logger.info("testTwoNodesOneLeaving: removing instance2 from the view - even though vsm1 didn't really know about it, it should send a TOPOLOGY_CHANGING - we leave it as deactivating for now...");
        DummyTopologyView oneLeaving = two1.clone();
        oneLeaving.removeInstance(slingId2);
        DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().setFinal(true).me(1).seq(1).activeIds(1).deactivatingIds(2));
        vsm1.handleNewView(oneLeaving);
        cs1.triggerBackgroundCheck();
        cs2.triggerBackgroundCheck();
        // wait for TOPOLOGY_CHANGING to be received by vsm1
        assertEquals(0, vsm1.waitForAsyncEvents(5000));
        assertEquals(2, l.countEvents());

        logger.info("testTwoNodesOneLeaving: marking instance2 as no longer deactivating, so vsm1 should now send a TOPOLOGY_CHANGED");
        DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().setFinal(true).me(1).seq(2).activeIds(1).inactiveIds(2));
        cs1.triggerBackgroundCheck();
        cs2.triggerBackgroundCheck();
        // wait for TOPOLOGY_CHANGED to be received by vsm1
        assertEquals(0, vsm1.waitForAsyncEvents(5000));
        RepositoryTestHelper.dumpRepo(factory1);
        assertEquals(3, l.countEvents());
    }
    
    @Test
    public void testRapidIdMapServiceActivateDeactivate() throws Exception {
        BackgroundCheckRunnable bgCheckRunnable = getBackgroundCheckRunnable(idMapService1);
        assertNotNull(bgCheckRunnable);
        assertFalse(bgCheckRunnable.isDone());
        idMapService1.deactivate();
        assertFalse(idMapService1.waitForInit(2500));
        bgCheckRunnable = getBackgroundCheckRunnable(idMapService1);
        assertNotNull(bgCheckRunnable);
        assertTrue(bgCheckRunnable.isDone());
    }
    
    private BackgroundCheckRunnable getBackgroundCheckRunnable(IdMapService idMapService) throws NoSuchFieldException, IllegalAccessException {
        Field field = idMapService.getClass().getSuperclass().getDeclaredField("backgroundCheckRunnable");
        field.setAccessible(true);
        Object backgroundCheckRunnable = field.get(idMapService);
        return (BackgroundCheckRunnable) backgroundCheckRunnable;
    }

    @Test
    public void testPartiallyStartedInstance() throws Exception {
        logger.info("testPartiallyStartedInstance: start");
        OakBacklogClusterSyncService cs = OakBacklogClusterSyncService.testConstructorAndActivate(new SimpleCommonsConfig(), idMapService1, new DummySlingSettingsService(slingId1), factory1);
        Lock lock = new ReentrantLock();
        ViewStateManager vsm = ViewStateManagerFactory.newViewStateManager(lock, cs);
        DummyListener l = new DummyListener();
        vsm.bind(l);
        vsm.handleActivated();

        final DummyTopologyView view1 = TestHelper.newView(true, slingId1, slingId1, slingId1);
        {
            // simulate a view with just itself (slingId1 / 1)
            vsm.handleNewView(view1);
            cs.triggerBackgroundCheck();
            assertEquals(0, vsm.waitForAsyncEvents(1000));
            assertEquals(0, l.countEvents());
            DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().me(1).seq(1).activeIds(1).setFinal(true));
            assertTrue(idMapService1.waitForInit(5000));
            cs.triggerBackgroundCheck();
            assertEquals(0, vsm.waitForAsyncEvents(1000));
            assertEquals(1, l.countEvents());
        }

        assertTrue(idMapService1.waitForInit(5000));

        {
            // simulate a new instance coming up - first it will show up in oak leases/lite-view
            DescriptorHelper.setDiscoveryLiteDescriptor(factory1, new DiscoveryLiteDescriptorBuilder().me(1).seq(2).activeIds(1, 2).setFinal(true));

            // the view is still the same (only contains slingId1) - but it has the flag 'partial' set
            final String syncToken2 = "s2";
            final String clusterId = view1.getLocalInstance().getClusterView().getId();
            final LocalClusterView cluster1Suppressed = new LocalClusterView(clusterId, syncToken2);
            final DummyTopologyView view1Suppressed = new DummyTopologyView(syncToken2)
                    .addInstance(slingId1, cluster1Suppressed, true, true);
            cluster1Suppressed.setPartiallyStartedClusterNodeIds(Arrays.asList(2));

            vsm.handleNewView(view1Suppressed);
            cs.triggerBackgroundCheck();
            assertEquals(0, vsm.waitForAsyncEvents(1000));
            assertEquals(1, l.countEvents());
        }
        final String slingId2 = UUID.randomUUID().toString();
        {
            // now define slingId for activeId == 2
            IdMapService idMapService2 = IdMapService.testConstructor(
                    new SimpleCommonsConfig(), new DummySlingSettingsService(slingId2), factory2);
            DescriptorHelper.setDiscoveryLiteDescriptor(factory2, new DiscoveryLiteDescriptorBuilder().setFinal(true).me(2).seq(2).activeIds(1, 2));
            assertTrue(idMapService2.waitForInit(5000));
        }
        {
            // now that shouldn't have triggered anything yet towards the listeners
            final String syncToken2 = "s2";
            final String clusterId = view1.getLocalInstance().getClusterView().getId();
            final LocalClusterView cluster1Suppressed = new LocalClusterView(clusterId, syncToken2);
            final DummyTopologyView view1Suppressed = new DummyTopologyView(syncToken2)
                    .addInstance(slingId1, cluster1Suppressed, true, true);
            cluster1Suppressed.setPartiallyStartedClusterNodeIds(Arrays.asList(2));

            vsm.handleNewView(view1Suppressed);
            cs.triggerBackgroundCheck();
            assertEquals(0, vsm.waitForAsyncEvents(1000));
            assertEquals(1, l.countEvents());
        }
        {
            // now let's finish slingId2 startup - only this should trigger CHANGING/CHANGED
            final String syncToken2 = "s2";
            final String clusterId = view1.getLocalInstance().getClusterView().getId();
            final LocalClusterView cluster1Suppressed = new LocalClusterView(clusterId, syncToken2);
            final DummyTopologyView view2 = new DummyTopologyView(syncToken2)
                    .addInstance(slingId1, cluster1Suppressed, true, true)
                    .addInstance(slingId2, cluster1Suppressed, false, false);
            vsm.handleNewView(view2);
            cs.triggerBackgroundCheck();
            assertEquals(0, vsm.waitForAsyncEvents(1000));
            assertEquals(3, l.countEvents());
        }
        logger.info("testPartiallyStartedInstance: end");
    }
}
