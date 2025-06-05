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

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryTestHelper {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryTestHelper.class);

    public static void dumpRepo(ResourceResolverFactory resourceResolverFactory) throws Exception {
        try (ResourceResolver resourceResolver = resourceResolverFactory.getServiceResourceResolver(null)) {
            Session session = resourceResolver.adaptTo(Session.class);
            logger.info("dumpRepo: ====== START =====");
            logger.info("dumpRepo: repo = " + session.getRepository());

            dump(session.getRootNode());

            logger.info("dumpRepo: ======  END  =====");
        }
    }

    public static void dump(Node node) throws RepositoryException {
        if (node.getPath().equals("/jcr:system")
                || node.getPath().equals("/rep:policy")) {
            // ignore that one
            return;
        }

        StringBuffer sb = new StringBuffer();
        sb.append(node.getPath() + " [" + node.getPrimaryNodeType().getName() + "] ");

        NodeIterator it = node.getNodes();
        while (it.hasNext()) {
            Node child = it.nextNode();
            sb.append(child.getName() + ", ");
        }
        logger.info("dump: " + sb.toString());
        it = node.getNodes();
        while (it.hasNext()) {
            Node child = it.nextNode();
            dump(child);
        }
    }


}
