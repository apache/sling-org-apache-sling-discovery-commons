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
package org.apache.sling.discovery.commons.providers.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

/**
 * Helper class to help reduce log.info output. It will avoid repetitive
 * log.info calls and instead log future occurrences to log.debug
 */
public class LogSilencer {

    private static final long DEFAULT_AUTO_RESET_DELAY_MINUTES = 10;

    private final Logger logger;

    private final Object syncObj = new Object();

    private final long autoResetDelayMillis;

    private Map<String, String> lastMsgPerCategory;

    private long autoResetTime = 0;

    public LogSilencer(Logger logger, long autoResetDelaySeconds) {
        this.logger = logger;
        if (autoResetDelaySeconds > 0) {
            autoResetDelayMillis = TimeUnit.MINUTES.toMillis(autoResetDelaySeconds);
        } else {
            autoResetDelayMillis = 0;
        }
    }

    public LogSilencer(Logger logger) {
        this(logger, DEFAULT_AUTO_RESET_DELAY_MINUTES);
    }

    public void infoOrDebug(String category, String msg) {
        final boolean doLogInfo;
        synchronized (syncObj) {
            if (autoResetTime == 0 || System.currentTimeMillis() > autoResetTime) {
                reset();
            }
            if (lastMsgPerCategory == null) {
                lastMsgPerCategory = new HashMap<>();
            }
            final String localLastMsg = lastMsgPerCategory.get(category);
            if (localLastMsg == null || !localLastMsg.equals(msg)) {
                doLogInfo = true;
                lastMsgPerCategory.put(category, msg);
            } else {
                doLogInfo = false;
            }
        }
        if (doLogInfo) {
            logger.info(msg + " (future identical logs go to debug)");
        } else {
            logger.debug(msg);
        }
    }

    public void infoOrDebug(String msg) {
        infoOrDebug(null, msg);
    }

    public void reset() {
        synchronized (syncObj) {
            lastMsgPerCategory = null;
            if (autoResetDelayMillis == 0) {
                autoResetTime = Long.MAX_VALUE;
            } else {
                autoResetTime = System.currentTimeMillis() + autoResetDelayMillis;
            }
        }
    }

}
