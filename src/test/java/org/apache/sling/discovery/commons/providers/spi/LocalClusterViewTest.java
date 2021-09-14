/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sling.discovery.commons.providers.spi;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalClusterViewTest {

    private final LocalClusterView view = new LocalClusterView("id", "token");

    @Test
    public void partiallyStarted() {
        assertFalse(view.hasPartiallyStartedInstances());
        assertFalse(view.isPartiallyStarted(42));

        view.setPartiallyStartedClusterNodeIds(Collections.<Integer>emptyList());
        assertFalse(view.hasPartiallyStartedInstances());
        assertFalse(view.isPartiallyStarted(42));

        view.setPartiallyStartedClusterNodeIds(Arrays.asList(1, 2, 3));
        assertTrue(view.hasPartiallyStartedInstances());
        assertFalse(view.isPartiallyStarted(42));
        assertTrue(view.isPartiallyStarted(1));
        assertFalse(view.isPartiallyStarted(null));
    }
}
