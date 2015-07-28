package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.configuration.webserver.controller.FileServer;
import org.apache.mesos.elasticsearch.systemtest.containers.ConfigurationContainer;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.util.concurrent.TimeUnit;

/**
 * Tests configuration project
 */
public class ConfigurationSystemTest extends TestBase {
    public static final Logger LOGGER = Logger.getLogger(ConfigurationSystemTest.class);

    @Test
    public void testServerAccessibleWithoutMesos() throws Exception {
        LOGGER.debug("Starting main");
        org.apache.mesos.elasticsearch.configuration.Main.main(new String[0]);
        Unirest.setProxy(null); // Reset proxy
        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(
                () -> {
                    Integer status = Unirest.get("http://localhost:" + "8080" + "/" + FileServer.BASE_URL + FileServer.HEALTH_URL).asString().getStatus();
                    LOGGER.debug("Status = " + status);
                    return status == HttpStatus.OK.value();
                }
        );
    }

    @Test
    public void testServerAccessibleWithMesos() throws InterruptedException {
        LOGGER.debug("Starting configuration container");
//        AbstractContainer container = new ConfigurationContainer(config.dockerClient);
        cluster.injectImage(ConfigurationContainer.IMAGE_NAME);
        LOGGER.debug("Configuration container started");
    }

    @Test
    public void testSchedulerStartConfiguration() {
        cluster.injectImage("mesos/elasticsearch-configuration");

    }
}
