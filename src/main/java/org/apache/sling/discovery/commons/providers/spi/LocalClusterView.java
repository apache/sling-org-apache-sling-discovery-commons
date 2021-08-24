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
package org.apache.sling.discovery.commons.providers.spi;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.sling.discovery.commons.providers.DefaultClusterView;

public class LocalClusterView extends DefaultClusterView {

    private final String localClusterSyncTokenId;
    private Set<Integer> partiallyStartedClusterNodeIds;

    public LocalClusterView(String id, String localClusterSyncTokenId) {
        super(id);
        this.localClusterSyncTokenId = localClusterSyncTokenId;
    }
    
    public String getLocalClusterSyncTokenId() {
        return localClusterSyncTokenId;
    }

    public void setPartiallyStartedClusterNodeIds(Collection<Integer> clusterNodeIds) {
        this.partiallyStartedClusterNodeIds = new HashSet<Integer>(clusterNodeIds);
    }

    public boolean isPartiallyStarted(Integer clusterNodeId) {
        if (partiallyStartedClusterNodeIds == null || clusterNodeId == null) {
            return false;
        }
        return partiallyStartedClusterNodeIds.contains(clusterNodeId);
    }

    public boolean hasPartiallyStartedInstances() {
        if (partiallyStartedClusterNodeIds == null) {
            return false;
        }
        return !partiallyStartedClusterNodeIds.isEmpty();
    }
}
