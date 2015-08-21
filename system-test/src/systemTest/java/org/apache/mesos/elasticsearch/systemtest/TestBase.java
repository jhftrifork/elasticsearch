package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Base test class which launches Mesos CLUSTER and Elasticsearch scheduler1
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public abstract class TestBase {

    protected static final int NODE_COUNT = 3;

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(NODE_COUNT)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    // Generality of tests is increased by always starting two frameworks, verifying that a single ES framework instance
    // works correctly in the presence of other ES frameworks.
    private static ElasticsearchSchedulerContainer scheduler1;
    private static ElasticsearchSchedulerContainer scheduler2;

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            LOGGER.info("TestWatcher.failed was called because: " + description.toString() + ". Shutting down schedulers.");

            CLUSTER.stop();

            LOGGER.info("Removing scheduler with id: " + scheduler1.getContainerId());
            scheduler1.remove();
            LOGGER.info("Removing scheduler with id: " + scheduler2.getContainerId());
            scheduler2.remove();
        }
    };

    @BeforeClass
    public static void startSchedulers() throws Exception {
        CLUSTER.injectImage("mesos/elasticsearch-executor");

        LOGGER.info("Starting Elasticsearch schedulers");

        scheduler1 = startSchedulerWithNameAndPort("elasticsearch-framework-1", "8080");
        scheduler2 = startSchedulerWithNameAndPort("elasticsearch-framework-2", "8081");
    }

    private static ElasticsearchSchedulerContainer startSchedulerWithNameAndPort(String name, String port) {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(CONFIG.dockerClient, CLUSTER.getMesosContainer().getIpAddress(), port, name);
        CLUSTER.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch instance with framework name \"" + name + "\" running on Docker container with id \"" + scheduler.getContainerId() + "\" listening on " + scheduler.getIpAddress() + ":" + port);
        return scheduler;
    }

    public static ElasticsearchSchedulerContainer getScheduler1() {
        return scheduler1;
    }

    public static ElasticsearchSchedulerContainer getScheduler2() {
        return scheduler2;
    }

}
