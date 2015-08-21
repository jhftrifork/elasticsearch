package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.Test;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Tests the main method.
 */
public class SchedulerMainSystemTest {
    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    private static final Logger LOGGER = Logger.getLogger(SchedulerMainSystemTest.class);

    @Test
    public void ensureMainFailsIfNoHeap() throws Exception {
        final String schedulerImage = "mesos/elasticsearch-scheduler";
        CreateContainerCmd createCommand = CONFIG.dockerClient
                .createContainerCmd(schedulerImage)
                .withCmd(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos", ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3", Configuration.ELASTICSEARCH_RAM, "256");

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CONFIG.dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            InputStream exec = CONFIG.dockerClient.logContainerCmd(containerId).withStdErr().exec();
            return !IOUtils.toString(exec).isEmpty();
        });
        InputStream exec = CONFIG.dockerClient.logContainerCmd(containerId).withStdErr().exec();
        String log = IOUtils.toString(exec);
        assertTrue(log.contains("Exception"));
        assertTrue(log.contains("heap"));
    }

    @Test
    public void ensureMainFailsIfInvalidHeap() throws Exception {
        final String schedulerImage = "mesos/elasticsearch-scheduler";
        CreateContainerCmd createCommand = CONFIG.dockerClient
                .createContainerCmd(schedulerImage)
                .withEnv("JAVA_OPTS=-Xms128s1m -Xmx256f5m")
                .withCmd(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos", ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3", Configuration.ELASTICSEARCH_RAM, "256");

        CreateContainerResponse r = createCommand.exec();
        String containerId = r.getId();
        StartContainerCmd startMesosClusterContainerCmd = CONFIG.dockerClient.startContainerCmd(containerId);
        startMesosClusterContainerCmd.exec();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            InputStream exec = CONFIG.dockerClient.logContainerCmd(containerId).withStdErr().exec();
            return !IOUtils.toString(exec).isEmpty();
        });
        InputStream exec = CONFIG.dockerClient.logContainerCmd(containerId).withStdErr().exec();
        String log = IOUtils.toString(exec);
        assertTrue(log.contains("Invalid initial heap size"));
    }

    @Test
    public void ensureMainWorksIfStartingSingleScheduler() throws Exception {
        ensureMainWorksIfStartingSchedulersWithIds("framework1");
    }

    @Test
    public void ensureMainWorksIfStartingTwoSchedulers() throws Exception {
        ensureMainWorksIfStartingSchedulersWithIds("framework1", "framework2");
    }

    private void ensureMainWorksIfStartingSchedulersWithIds(String... frameworkIds) {
        Map<String, String> frameworkIdToContainerId = new HashMap<>();
        for (String frameworkId : frameworkIds) {
            frameworkIdToContainerId.put(frameworkId, createSchedulerWithFrameworkName(frameworkId));
        }
        for (String containerId : frameworkIdToContainerId.values()) {
            CONFIG.dockerClient.startContainerCmd(containerId).exec();
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            Collection<String> waitingForContainerIds = frameworkIdToContainerId.values().stream().collect(Collectors.toSet());
            LOGGER.info("Expecting containers to start: " + waitingForContainerIds);
            Set<String> startedContainerIds = CONFIG.dockerClient.listContainersCmd().exec().stream().map(Container::getId).collect(Collectors.toSet());
            LOGGER.info("Containers that have started: " + startedContainerIds.toString());
            Set<String> notYetStartedContainerIds = CollectionUtils.subtract(waitingForContainerIds, startedContainerIds).stream().collect(Collectors.toSet());
            LOGGER.info("Not yet started: " + notYetStartedContainerIds.toString());
            return notYetStartedContainerIds.isEmpty();
        });
    }

    private String createSchedulerWithFrameworkName(String frameworkName) {
        return CONFIG
                .dockerClient
                .createContainerCmd("mesos/elasticsearch-scheduler")
                .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                .withCmd(
                        ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "zk://" + "noIP" + ":2181/mesos",
                        ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                        Configuration.ELASTICSEARCH_RAM, "256",
                        "--frameworkName", frameworkName
                )
                .exec()
                .getId();
    }
}
