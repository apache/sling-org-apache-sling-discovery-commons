package org.apache.sling.discovery.commons.providers.spi.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.List;

import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.DummyTopologyView;
import org.apache.sling.discovery.commons.providers.spi.ClusterSyncService;
import org.junit.Before;
import org.junit.Test;

public class TestClusterSyncServiceChain {

    class SimpleClusterSyncService implements ClusterSyncService {

        volatile boolean cancelled = false;

        @Override
        public void sync(BaseTopologyView view, Runnable callback) {
            callback.run();
        }

        @Override
        public void cancelSync() {
            cancelled = true;
        }

    }

    class SimpleRunnable implements Runnable {

        @Override
        public void run() {
            // nothing done here so far
        }

    }

    ClusterSyncServiceChain chain;
    SimpleClusterSyncService[] elements;
    BaseTopologyView view;
    Runnable callback;

    @Before
    public void setup() {
        view = new DummyTopologyView();
        callback = new SimpleRunnable();
    }

    private void initChain(int numElements) {
        final List<SimpleClusterSyncService> l = new LinkedList<>();
        for (int i = 0; i < numElements; i++) {
            l.add(new SimpleClusterSyncService());
        }
        elements = l.toArray(new SimpleClusterSyncService[l.size()]);
        chain = new ClusterSyncServiceChain(elements);
    }

    @Test
    public void testNulls() {
        initChain(2);
        try {
            chain.sync(null, null);
            fail("should complain");
        } catch (RuntimeException npe) {
            // ok
        }
        try {
            chain.sync(view, null);
            fail("should complain");
        } catch (RuntimeException npe) {
            // ok
        }
        try {
            chain.sync(null, callback);
            fail("should complain");
        } catch (RuntimeException npe) {
            // ok
        }
    }

    @Test
    public void testCancel() {
        initChain(3);
        assertFalse(elements[0].cancelled);
        assertFalse(elements[1].cancelled);
        assertFalse(elements[1].cancelled);
        chain.cancelSync();
        assertTrue(elements[0].cancelled);
        assertTrue(elements[1].cancelled);
        assertTrue(elements[1].cancelled);
        chain.cancelSync();
        assertTrue(elements[0].cancelled);
        assertTrue(elements[1].cancelled);
        assertTrue(elements[1].cancelled);
    }

    @Test
    public void testSyncCancel() {
        initChain(2);
        chain.sync(view, callback);
        chain.cancelSync();
        chain.cancelSync();
    }

    @Test
    public void testMultipleSyncCancel() {
        initChain(3);
        chain.sync(view, callback);
        chain.sync(view, callback);
        chain.sync(view, callback);
        chain.cancelSync();
        chain.cancelSync();
    }

    private static DummyClusterSyncService newSyncService(String debugName, int checkPermits, boolean result) {
        final long timeoutMillis = 10000;
        final long intervalMillis = 10;
        DummyClusterSyncService elem1 = new DummyClusterSyncService(timeoutMillis, intervalMillis, debugName);
        elem1.setCheckSemaphoreSetPermits(checkPermits);
        elem1.setCheckResult(result);
        return elem1;
    }

    /**
     * Tests the background thread of the pair ClusterSyncServiceChain /
     * AbstractServiceWithBackgroundCheck.
     * <p/>
     * This variant checks without any test-blocking going on
     */
    @Test
    public void testBackgroundThreads_simpleUnblocked() {
        DummyClusterSyncService elem1 = newSyncService("elem1", Integer.MAX_VALUE, true);
        DummyClusterSyncService elem2 = newSyncService("elem2", Integer.MAX_VALUE, true);
        chain = new ClusterSyncServiceChain(elem1, elem2);

        // should not be blocking at the start
        elem1.waitForCheckBlockingAtMax(0, 10);
        elem2.waitForCheckBlockingAtMax(0, 10);
        // counters should all be at zero at the start
        assertEquals(0, elem1.getCheckCounter());
        assertEquals(0, elem2.getCheckCounter());

        chain.sync(view, callback);

        // should not be blocking even after a sync
        assertTrue(elem1.waitForCheckBlockingAtMax(0, 50));
        assertTrue(elem2.waitForCheckBlockingAtMax(0, 50));
        // but counters should now be at 1 for each
        assertTrue(elem1.waitForCheckCounterAtMin(1, 5000));
        assertTrue(elem2.waitForCheckCounterAtMin(1, 5000));
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());

        // canceling shouldn't have any influence here
        chain.cancelSync();
        assertEquals(1, elem1.getCheckCounter());
        assertEquals(1, elem2.getCheckCounter());
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());
    }

    /**
     * Tests the background thread of the pair ClusterSyncServiceChain /
     * AbstractServiceWithBackgroundCheck.
     * <p/>
     * This variant checks simple test-blocking - kind of ensures the test framework
     * behaves as expected (nothing much of interesting assertions here yet)
     */
    @Test
    public void testBackgroundThreads_simpleBlocked() throws InterruptedException {
        DummyClusterSyncService elem1 = newSyncService("elem1", Integer.MAX_VALUE, false);
        DummyClusterSyncService elem2 = newSyncService("elem2", Integer.MAX_VALUE, false);
        chain = new ClusterSyncServiceChain(elem1, elem2);
        chain.sync(view, callback);
        assertTrue(elem1.waitForCheckBlockingAtMax(0, 50));
        assertTrue(elem2.waitForCheckBlockingAtMax(0, 50));
        elem1.setCheckResult(true);
        assertTrue(elem1.waitForCheckBlockingAtMax(1, 5000));
        assertTrue(elem2.waitForCheckBlockingAtMax(0, 50));
        elem2.setCheckResult(true);
        assertTrue(elem1.waitForCheckCounterAtMin(1, 5000));
        assertTrue(elem2.waitForCheckCounterAtMin(1, 5000));
        chain.cancelSync();
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());
    }

    /**
     * Tests the background thread of the pair ClusterSyncServiceChain /
     * AbstractServiceWithBackgroundCheck.
     * <p/>
     * This variant checks blocking/releasing of the first elem
     */
    @Test
    public void testBackgroundThreads_blockElem1Bg1() throws Exception {
        DummyClusterSyncService elem1 = newSyncService("elem1", Integer.MAX_VALUE, false);
        DummyClusterSyncService elem2 = newSyncService("elem2", Integer.MAX_VALUE, false);
        chain = new ClusterSyncServiceChain(elem1, elem2);
        chain.sync(view, callback);

        // let's have some background looping going on for elem1 first,
        // ie we wait for the counter to reach 5
        assertTrue(elem1.waitForCheckCounterAtMin(5, 5000));
        // elem2 though should not have gotten any check calls yet
        // as elem1's check is stillf ailing
        assertEquals(0, elem2.getCheckCounter());

        // now halt elem1's bg thread
        elem1.setCheckSemaphoreSetPermits(0);
        assertTrue(elem1.waitForCheckBlockingAtMin(1, 5000));
        assertEquals(0, elem2.getCheckCounter());

        chain.cancelSync();

        // after the canceling - with elem1's bg thread blocked -
        // let it succeed with 'true'
        elem1.setCheckResult(true);
        elem1.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);

        // now ensure things work fine, elem1 and elem2 both terminate
        assertTrue(elem1.waitForBackgroundCheckFinished(5000));
        assertTrue(elem2.waitForBackgroundCheckFinished(5000));
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());

        // elem2 should not have gotten any check calls though
        assertEquals(0, elem2.getCheckCounter());
    }

    /**
     * Tests the background thread of the pair ClusterSyncServiceChain /
     * AbstractServiceWithBackgroundCheck.
     * <p/>
     * This variant checks blocking/releasing of the elem2 when in a bg thread of
     * elem1
     */
    @Test
    public void testBackgroundThreads_blockElem2Bg1() throws Exception {
        DummyClusterSyncService elem1 = newSyncService("elem1", Integer.MAX_VALUE, false);
        DummyClusterSyncService elem2 = newSyncService("elem2", Integer.MAX_VALUE, false);
        chain = new ClusterSyncServiceChain(elem1, elem2);
        chain.sync(view, callback);

        // go through a controlled step-wise flow:
        // first let elem1 get into the background thread and do calls
        assertTrue(elem1.waitForCheckCounterAtMin(5, 5000));
        assertEquals(0, elem2.getCheckCounter());

        // halt elem1
        elem1.setCheckSemaphoreSetPermits(0);
        assertTrue(elem1.waitForCheckBlockingAtMin(1, 5000));
        assertEquals(0, elem2.getCheckCounter());

        // then unblock elem1 with result 'true' and block elem2 instead
        elem1.setCheckResult(true);
        // the blocking should happen immediately - ie not in a bg thread yet
        elem2.setCheckSemaphoreSetPermits(0);
        elem1.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);

        // wait until the blocking has happened
        assertTrue(elem2.waitForCheckBlockingAtMin(1, 5000));
        assertEquals(0, elem2.getCheckCounter());
        assertTrue(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());

        // now cancel
        chain.cancelSync();

        assertTrue(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());

        // release elem1's bg thread calling elem2's check
        elem2.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);

        // wait for the unblocking of the elem2's check to have happened
        assertTrue(elem2.waitForCheckCounterAtMin(1, 5000));

        // now wait for elem1's bg thread to have finished
        // and elem2 shouldn't even have started
        assertTrue(elem1.waitForBackgroundCheckFinished(5000));
        assertTrue(elem2.waitForBackgroundCheckFinished(5000));
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());
    }

    /**
     * Tests the background thread of the pair ClusterSyncServiceChain /
     * AbstractServiceWithBackgroundCheck.
     * <p/>
     * This variant checks blocking/releasing of the elem2 when in a bg thread of
     * elem2 was just started
     */
    @Test
    public void testBackgroundThreads_blockElem2Bg2Started() throws Exception {
        DummyClusterSyncService elem1 = newSyncService("elem1", Integer.MAX_VALUE, false);
        DummyClusterSyncService elem2 = newSyncService("elem2", Integer.MAX_VALUE, false);
        chain = new ClusterSyncServiceChain(elem1, elem2);
        chain.sync(view, callback);

        // go through a controlled step-wise flow:
        // first let elem1 get into the background thread and do calls
        assertTrue(elem1.waitForCheckCounterAtMin(5, 5000));
        assertEquals(0, elem2.getCheckCounter());

        // halt elem1
        elem1.setCheckSemaphoreSetPermits(0);
        assertTrue(elem1.waitForCheckBlockingAtMin(1, 5000));
        assertEquals(0, elem2.getCheckCounter());

        // then unblock elem1 with result 'true' and block elem2 instead
        elem1.setCheckResult(true);
        // the blocking should happen after 1 call, so the bg thread just started
        elem2.setCheckSemaphoreSetPermits(1);
        elem1.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);

        // wait until the blocking has happened
        assertTrue(elem2.waitForCheckBlockingAtMin(1, 5000));
        assertEquals(1, elem2.getCheckCounter());
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertTrue(elem2.hasBackgroundCheckRunnable());

        // now cancel
        chain.cancelSync();

        // release elem2's bg thread
        elem2.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);

        // wait for the unblocking of the elem2's check to have happened
        assertTrue(elem2.waitForBackgroundCheckFinished(5000));

        // check states
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());
        assertEquals(2, elem2.getCheckCounter());
    }

    /**
     * Tests the background thread of the pair ClusterSyncServiceChain /
     * AbstractServiceWithBackgroundCheck.
     * <p/>
     * This variant checks blocking/releasing of the elem2 when in a bg thread of
     * elem2 that was looping a little while
     */
    @Test
    public void testBackgroundThreads_blockElem2Bg2Looping() throws Exception {
        DummyClusterSyncService elem1 = newSyncService("elem1", Integer.MAX_VALUE, false);
        DummyClusterSyncService elem2 = newSyncService("elem2", Integer.MAX_VALUE, false);
        chain = new ClusterSyncServiceChain(elem1, elem2);
        chain.sync(view, callback);

        // go through a controlled step-wise flow:
        // first let elem1 get into the background thread and do calls
        assertTrue(elem1.waitForCheckCounterAtMin(5, 5000));
        assertEquals(0, elem2.getCheckCounter());

        // halt elem1
        elem1.setCheckSemaphoreSetPermits(0);
        assertTrue(elem1.waitForCheckBlockingAtMin(1, 5000));
        assertEquals(0, elem2.getCheckCounter());

        // then unblock elem1 with result 'true' and block elem2 instead
        elem1.setCheckResult(true);
        // the blocking should happen after 5 calls
        elem2.setCheckSemaphoreSetPermits(5);
        elem1.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);

        // wait until elem2 is checked by bg thread 5 times
        assertTrue(elem2.waitForCheckCounterAtMin(5, 5000));
        // then ensure we reached the blocking semaphore with elem2
        assertTrue(elem2.waitForCheckBlockingAtMin(1, 5000));

        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertTrue(elem2.hasBackgroundCheckRunnable());

        // now cancel
        chain.cancelSync();

        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertTrue(elem2.hasBackgroundCheckRunnable());

        // release elem2's bg thread
        elem2.setCheckSemaphoreSetPermits(Integer.MAX_VALUE);

        // now check for elem2's bg thread to be finished
        assertTrue(elem2.waitForBackgroundCheckFinished(5000));
        assertFalse(elem1.hasBackgroundCheckRunnable());
        assertFalse(elem2.hasBackgroundCheckRunnable());

        // no call should have happened to elem2's check
        assertEquals(6, elem2.getCheckCounter());
    }
}
