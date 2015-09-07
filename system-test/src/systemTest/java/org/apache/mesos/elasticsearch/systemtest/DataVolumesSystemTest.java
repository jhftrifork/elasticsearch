package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.TaskInfoFactory;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests data volumes
 */
public class DataVolumesSystemTest extends TestBase {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

    @Test
    public void testDataVolumes() {
        ElasticsearchSchedulerContainer scheduler = getScheduler1();

        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), scheduler.getManagementPort(), NODE_COUNT);

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, NODE_COUNT);
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());

        ExecCreateCmdResponse execResponse = CONFIG.dockerClient.execCreateCmd(CLUSTER.getMesosContainer().getContainerId())
                .withCmd("ls", "-R", TaskInfoFactory.SETTINGS_DATA_VOLUME_HOST)
                .withTty(true)
                .withAttachStderr()
                .withAttachStdout()
                .exec();
        try (InputStream inputstream = CONFIG.dockerClient.execStartCmd(CLUSTER.getMesosContainer().getContainerId()).withTty().withExecId(execResponse.getId()).exec()) {
            String contents = IOUtils.toString(inputstream);
            LOGGER.info("Mesos-local contents of " + TaskInfoFactory.SETTINGS_DATA_VOLUME_HOST + "/elasticsearch/nodes: " + contents);
            assertTrue(contents.contains("0"));
            assertTrue(contents.contains("1"));
            assertTrue(contents.contains("2"));
        } catch (IOException e) {
            LOGGER.error("Could not list contents of " + TaskInfoFactory.SETTINGS_DATA_VOLUME_HOST + "/elasticsearch/nodes in Mesos-Local");
        }
    }

}
