/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.NoHttpResponseException;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestClusterTest {

    protected static final String STATUS_FORBIDDEN = "{\"status\":\"forbidden\"}";

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config createConfig() {
        return smallInstanceConfig();
    }

    protected Config createConfigWithRestEnabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected String getPassword() {
        // Community version doesn't check the password.
        return "";
    }

    @Test
    public void testDisabledRest() throws Exception {
        // REST should be disabled by default
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getClusterInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Test
    public void testClusterShutdown() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance2);


        String response = communicator.shutdownCluster(config.getGroupConfig().getName(), getPassword()).response;
        assertThat(response, CoreMatchers.containsString("\"status\":\"success\""));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(instance1.getLifecycleService().isRunning());
                assertFalse(instance2.getLifecycleService().isRunning());
            }
        });
    }

    @Test
    public void testGetClusterState() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        String groupName = config.getGroupConfig().getName();
        HTTPCommunicator communicator1 = new HTTPCommunicator(instance1);
        HTTPCommunicator communicator2 = new HTTPCommunicator(instance2);

        instance1.getCluster().changeClusterState(ClusterState.FROZEN);
        assertEquals("{\"status\":\"success\",\"state\":\"frozen\"}",
                communicator1.getClusterState(groupName, getPassword()));

        instance1.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertEquals("{\"status\":\"success\",\"state\":\"passive\"}",
                communicator2.getClusterState(groupName, getPassword()));
    }

    @Test
    public void testChangeClusterState() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);
        String groupName = config.getGroupConfig().getName();

        assertEquals(STATUS_FORBIDDEN, communicator.changeClusterState(groupName + "1", getPassword(), "frozen").response);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.changeClusterState(groupName, getPassword(), "frozen").responseCode);

        assertClusterStateEventually(ClusterState.FROZEN, instance1);
        assertClusterStateEventually(ClusterState.FROZEN, instance2);
    }

    @Test
    public void testGetClusterVersion() throws IOException {
        final HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String expected = "{\"status\":\"success\","
                + "\"version\":\"" + instance.getCluster().getClusterVersion().toString() + "\"}";
        assertEquals(expected, communicator.getClusterVersion());
    }

    @Test
    public void testChangeClusterVersion() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String groupName = config.getGroupConfig().getName();
        assertEquals(HttpURLConnection.HTTP_OK, communicator.changeClusterVersion(groupName, getPassword(),
                instance.getCluster().getClusterVersion().toString()).responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.changeClusterVersion(groupName + "1", getPassword(), "1.2.3").response);
    }

    @Test
    public void testHotBackup() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String groupName = config.getGroupConfig().getName();
        assertEquals(HttpURLConnection.HTTP_OK, communicator.hotBackup(groupName, getPassword()).responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.hotBackup(groupName + "1", getPassword()).response);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.hotBackupInterrupt(groupName, getPassword()).responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.hotBackupInterrupt(groupName + "1", getPassword()).response);
    }

    @Test
    public void testForceAndPartialStart() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String groupName = config.getGroupConfig().getName();
        assertEquals(HttpURLConnection.HTTP_OK, communicator.forceStart(groupName, getPassword()).responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.forceStart(groupName + "1", getPassword()).response);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.partialStart(groupName, getPassword()).responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.partialStart(groupName + "1", getPassword()).response);
    }

    @Test
    public void testManagementCenterUrlChange() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String groupName = config.getGroupConfig().getName();
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT,
                communicator.changeManagementCenterUrl(groupName, getPassword(), "http://bla").responseCode);
    }

    @Test
    public void testListNodes() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HazelcastTestSupport.waitInstanceForSafeState(instance);
        String result = String.format("{\"status\":\"success\",\"response\":\"[%s]\n%s\n%s\"}",
                instance.getCluster().getLocalMember().toString(),
                BuildInfoProvider.getBuildInfo().getVersion(),
                System.getProperty("java.version"));
        String groupName = config.getGroupConfig().getName();
        assertEquals(result, communicator.listClusterNodes(groupName, getPassword()));
    }

    @Test
    public void testListNodesWithWrongCredentials() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);
        HazelcastTestSupport.waitInstanceForSafeState(instance1);
        String groupName = config.getGroupConfig().getName();
        assertEquals(STATUS_FORBIDDEN, communicator.listClusterNodes(groupName + "1", getPassword()));
    }

    @Test
    public void testShutdownNode() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        instance.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                    shutdownLatch.countDown();
                }
            }
        });
        String groupName = config.getGroupConfig().getName();
        try {
            assertEquals("{\"status\":\"success\"}", communicator.shutdownMember(groupName, getPassword()));
        } catch (SocketException ignored) {
            // if the node shuts down before response is received, a `SocketException` (or instance of its subclass) is expected
        } catch (NoHttpResponseException ignored) {
            // `NoHttpResponseException` is also a possible outcome when a node shut down before it has a chance
            // to send a response back to a client.
        }


        assertOpenEventually(shutdownLatch);
        assertFalse(instance.getLifecycleService().isRunning());
    }

    @Test
    public void testShutdownNodeWithWrongCredentials() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String groupName = config.getGroupConfig().getName();
        assertEquals(STATUS_FORBIDDEN, communicator.shutdownMember(groupName + "1", getPassword()));
    }

    @Test
    public void simpleHealthCheck() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String result = communicator.getClusterHealth();

        assertEquals("Hazelcast::NodeState=ACTIVE\n"
                + "Hazelcast::ClusterState=ACTIVE\n"
                + "Hazelcast::ClusterSafe=TRUE\n"
                + "Hazelcast::MigrationQueueSize=0\n"
                + "Hazelcast::ClusterSize=1\n", result);
    }

    @Test
    public void healthCheckWithPathParameters() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals("ACTIVE", communicator.getClusterHealth("/node-state"));
        assertEquals("ACTIVE", communicator.getClusterHealth("/cluster-state"));
        assertEquals(HttpURLConnection.HTTP_OK, communicator.getClusterHealthResponseCode("/cluster-safe"));
        assertEquals("0", communicator.getClusterHealth("/migration-queue-size"));
        assertEquals("1", communicator.getClusterHealth("/cluster-size"));
    }

    @Test
    public void healthCheckWithUnknownPathParameter() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.getClusterHealthResponseCode("/unknown-parameter"));
    }

    @Test(expected = IOException.class)
    public void fail_with_deactivatedHealthCheck() throws Exception {
        // Healthcheck REST URL is deactivated by default - no passed config on purpose
        HazelcastInstance instance = factory.newHazelcastInstance(null);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        communicator.getClusterHealth();
    }

    @Test
    public void fail_on_healthcheck_url_with_garbage() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.getFailingClusterHealthWithTrailingGarbage());
    }

    @Test
    public void testHeadRequest_ClusterVersion() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterVersionURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterInfo() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterInfoURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterHealth() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator.headRequestToClusterHealthURI();
        assertEquals(HttpURLConnection.HTTP_OK, response.responseCode);
        assertEquals(response.responseHeaders.get("Hazelcast-NodeState").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-NodeState"), "ACTIVE");
        assertEquals(response.responseHeaders.get("Hazelcast-ClusterState").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-ClusterState"), "ACTIVE");
        assertEquals(response.responseHeaders.get("Hazelcast-ClusterSize").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-ClusterSize"), "2");
        assertEquals(response.responseHeaders.get("Hazelcast-MigrationQueueSize").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-MigrationQueueSize"), "0");
    }

    @Test
    public void testHeadRequest_GarbageClusterHealth() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, communicator.headRequestToGarbageClusterHealthURI().responseCode);
    }

    @Test
    public void http_get_returns_response_code_200_when_member_is_ready_to_use() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        int healthReadyResponseCode = communicator.getHealthReadyResponseCode();
        assertEquals(HttpURLConnection.HTTP_OK, healthReadyResponseCode);
    }
}
