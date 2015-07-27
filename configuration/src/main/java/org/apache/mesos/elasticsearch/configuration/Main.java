package org.apache.mesos.elasticsearch.configuration;

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.configuration.mesos.ConfigurationExecutor;
import org.apache.mesos.elasticsearch.configuration.mesos.TaskStatus;

/**
 * Main for webserver for ES configuration
 */
public class Main {
    public static void main(String[] args) throws Exception {
        MesosExecutorDriver driver = new MesosExecutorDriver(new ConfigurationExecutor(new TaskStatus()));
        System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
    }
}
