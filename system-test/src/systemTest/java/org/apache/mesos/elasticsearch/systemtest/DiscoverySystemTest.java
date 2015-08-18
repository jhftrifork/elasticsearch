package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.mini.state.Framework;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

/**
 * Tests REST node discovery
 */
public class DiscoverySystemTest extends TestBase {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

    @Test
    public void testSchedulerRegistration() {
        await().atMost(60, TimeUnit.SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ArrayList<Framework> frameworks = CLUSTER.getStateInfo().getFrameworks();
                LOGGER.info("Waiting for two frameworks in cluster; got: " + frameworks.toString());
                frameworks.stream().anyMatch((Framework f) -> f.getName() == getScheduler1().getName());
                return frameworks.size() == 2;
            }
        });
    }

    @Test
    public void testNodeDiscoveryRest() throws InterruptedException {
        testNodeDiscoveryRestForScheduler(getScheduler1());
        testNodeDiscoveryRestForScheduler(getScheduler2());
    }

    private void testNodeDiscoveryRestForScheduler(ElasticsearchSchedulerContainer scheduler) {
        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), scheduler.getManagementPort(), NODE_COUNT);

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, NODE_COUNT);
        assertTrue("Elasticsearch nodes for framework \"" + scheduler.getFrameworkName() + "\" did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }
}
