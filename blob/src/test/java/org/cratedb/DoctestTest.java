/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb;

import org.cratedb.test.integration.CrateIntegrationTest;
import org.cratedb.test.integration.DoctestClusterTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;


@CrateIntegrationTest.ClusterScope(numNodes = 2, scope = CrateIntegrationTest.Scope.GLOBAL)
public class DoctestTest extends DoctestClusterTestCase {

    @Test
    public void testBlob() throws Exception {

        client().admin().indices().prepareCreate("test")
            .setSettings(
                ImmutableSettings.builder()
                    .put("blobs.enabled", true)
                    .put("number_of_shards", 2)
                    .put("number_of_replicas", 0).build()).execute().actionGet();

        client().admin().indices().prepareCreate("test_blobs2")
            .setSettings(
                ImmutableSettings.builder()
                    .put("blobs.enabled", true)
                    .put("number_of_shards", 2)
                    .put("number_of_replicas", 0).build()).execute().actionGet();

        client().admin().indices().prepareCreate("test_no_blobs")
            .setSettings(
                ImmutableSettings.builder()
                .put("blobs.enabled", false)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build()).execute().actionGet();

        execDocFile("integrationtests/blob.rst", getClass());
    }

}
