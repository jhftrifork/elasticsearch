package org.apache.mesos.elasticsearch.configuration.mesos;


import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.configuration.webserver.WebApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.Arrays;

/**
 * Mesos executor for webserver
 */
public class ConfigurationExecutor  implements Executor {
    private static final Logger LOGGER = Logger.getLogger(ConfigurationExecutor.class.getSimpleName());

    private final TaskStatus taskStatus;

    public ConfigurationExecutor(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Configuration registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Configuration re-registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("Executor Configuration disconnected");
    }

    @Override
    public void launchTask(ExecutorDriver driver, final Protos.TaskInfo task) {
        LOGGER.info("Starting executor with a TaskInfo of:");
        LOGGER.info(task.toString());

        Protos.TaskID taskID = task.getTaskId();
        taskStatus.setTaskID(taskID);

        // Send status update, starting
        driver.sendStatusUpdate(taskStatus.starting());

        new SpringApplicationBuilder(WebApplication.class)
                .showBanner(false)
                .run();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                // Send status update, finished
                driver.sendStatusUpdate(taskStatus.finished());
            }
        }));

        // Send status update, running
        driver.sendStatusUpdate(taskStatus.running());
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
        driver.sendStatusUpdate(taskStatus.killed());
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.info("Framework message: " + Arrays.toString(data));
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutdown task: ");
        driver.sendStatusUpdate(taskStatus.finished());
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error task: " + message);
        driver.sendStatusUpdate(taskStatus.error());
    }
}
