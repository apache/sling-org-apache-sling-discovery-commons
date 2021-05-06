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
package org.apache.sling.discovery.commons.providers;

import org.apache.sling.discovery.commons.providers.spi.base.DiscoveryLiteConfig;

public final class SimpleCommonsConfig implements DiscoveryLiteConfig {

    private static final String SYNCTOKEN_PATH = "/var/discovery/commons/synctokens";
    private static final String IDMAP_PATH = "/var/discovery/commons/idmap";
    private long bgIntervalMillis;
    private long bgTimeoutMillis;

    public SimpleCommonsConfig() {
        this(1000, -1); // defaults
    }

    SimpleCommonsConfig(long bgIntervalMillis, long bgTimeoutMillis) {
        this.bgIntervalMillis = bgIntervalMillis;
        this.bgTimeoutMillis = bgTimeoutMillis;
    }

    @Override
    public String getSyncTokenPath() {
        return SYNCTOKEN_PATH;
    }

    @Override
    public String getIdMapPath() {
        return IDMAP_PATH;
    }

    @Override
    public long getClusterSyncServiceTimeoutMillis() {
        return bgTimeoutMillis;
    }

    @Override
    public long getClusterSyncServiceIntervalMillis() {
        return bgIntervalMillis;
    }

}