package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests REST node discovery
 */
public class DiscoverySystemTest extends TestBase {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

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
