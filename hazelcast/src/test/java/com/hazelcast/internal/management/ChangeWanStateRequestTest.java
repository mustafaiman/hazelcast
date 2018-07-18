/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.management;

import com.hazelcast.config.WanPublisherState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.request.ChangeWanStateRequest;
import com.hazelcast.json.Json;
import com.hazelcast.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.WanPublisherState.PAUSED;
import static com.hazelcast.config.WanPublisherState.REPLICATING;
import static com.hazelcast.config.WanPublisherState.STOPPED;
import static com.hazelcast.util.JsonUtil.getString;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChangeWanStateRequestTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Test
    public void testResumingWanState() {
        testChangeState(REPLICATING);
    }

    @Test
    public void testPausingWanState() {
        testChangeState(PAUSED);
    }

    @Test
    public void testStoppingWanState() {
        testChangeState(STOPPED);
    }

    private void testChangeState(WanPublisherState replicating) {
        ChangeWanStateRequest changeWanStateRequest = new ChangeWanStateRequest("schema", "publisher", replicating);
        JsonObject jsonObject = Json.object();
        changeWanStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertNotEquals(ChangeWanStateRequest.SUCCESS, getString(result, "result"));
    }
}