package org.apache.mesos.elasticsearch.systemtest;

import com.google.common.collect.ImmutableSet;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.state.Framework;
import org.json.JSONObject;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
                Set<String> expectedFrameworkNames = ImmutableSet.of(getScheduler1().getFrameworkName(), getScheduler2().getFrameworkName());
                Set<String> actualFrameworkNames = CLUSTER.getStateInfo().getFrameworks().stream().map((Framework f) -> f.getName()).collect(Collectors.toSet());
                LOGGER.info("Waiting for our two frameworks in the cluster; got: " + actualFrameworkNames.toString());
                return setsEqual(expectedFrameworkNames, actualFrameworkNames);
            }
        });
    }

    private static <T> boolean setsEqual(Set<T> s1, Set<T> s2) {
        return s1.containsAll(s2) && s2.containsAll(s1);
    }

    @Test
    public void testNodeDiscoveryRest() throws InterruptedException {
        testNodeDiscoveryRestForScheduler(getScheduler1());
        testNodeDiscoveryRestForScheduler(getScheduler2());
    }

    private void testNodeDiscoveryRestForScheduler(ElasticsearchSchedulerContainer scheduler) {
        List<JSONObject> tasks = new TasksResponse(scheduler.getIpAddress(), scheduler.getManagementPort(), NODE_COUNT).getTasks();
        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, NODE_COUNT);
        assertTrue("Elasticsearch nodes for framework \"" + scheduler.getFrameworkName() + "\" did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }
}
