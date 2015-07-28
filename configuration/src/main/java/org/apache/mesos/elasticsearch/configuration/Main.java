package org.apache.mesos.elasticsearch.configuration;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.configuration.mesos.ConfigurationExecutor;
import org.apache.mesos.elasticsearch.configuration.mesos.TaskStatus;
import org.apache.mesos.elasticsearch.configuration.webserver.WebApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Main for webserver for ES configuration
 */
public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        LOGGER.warn("Attempting to start configuration webserver as executor task");
        try {
            MesosExecutorDriver driver = new MesosExecutorDriver(new ConfigurationExecutor(new TaskStatus()));
            System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
        } catch (UnsatisfiedLinkError ex) {
            LOGGER.warn("Starting configuration image outside of executor");
            new SpringApplicationBuilder(WebApplication.class)
                .showBanner(false)
                .run();
        }
    }
}
