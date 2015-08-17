package org.apache.mesos.elasticsearch.executor.mesos;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.executor.Configuration;
import org.apache.mesos.elasticsearch.executor.elasticsearch.Launcher;
import org.apache.mesos.elasticsearch.executor.model.PortsModel;
import org.apache.mesos.elasticsearch.executor.model.RunTimeSettings;
import org.apache.mesos.elasticsearch.executor.model.ZooKeeperModel;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;

/**
 * Executor for Elasticsearch.
 *
 * This only behaves correctly in an environment where Mesos will launch an executor for each new task.
 */
public class ElasticsearchExecutor implements Executor {
    private final Launcher launcher;
    public static final Logger LOGGER = Logger.getLogger(ElasticsearchExecutor.class.getCanonicalName());
    private final TaskStatus taskStatus;
    private Configuration configuration;

    public ElasticsearchExecutor(Launcher launcher) {
        this.launcher = launcher;
        this.taskStatus = new TaskStatus();
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info(
            "Executor \"" + executorInfo.getName() + "\" " +
            "for framework \"" + frameworkInfo.getId().getValue() + "\" " +
            "(framework name \"" + frameworkInfo.getName() + "\") " +
            "registered on slave \"" + slaveInfo.getId() + "\" " +
            "(hostname \"" + slaveInfo.getHostname() + "\")"
        );
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info(
            "Executor re-registered on slave \"" + slaveInfo.getId() + "\" " +
            "(hostname \"" + slaveInfo.getHostname() + "\")"
        );
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("Executor Elasticsearch disconnected");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        LOGGER.info("Starting task with a TaskInfo of:\n" + task.toString());

        Protos.TaskID taskID = task.getTaskId();

        taskStatus.setTaskState(taskID, Protos.TaskState.TASK_STARTING, driver);

        try {
            // Parse CommandInfo arguments
            List<String> list = task.getExecutor().getCommand().getArgumentsList();
            String[] args = list.toArray(new String[list.size()]);
            LOGGER.debug("Using arguments: " + Arrays.toString(args));
            configuration = new Configuration(args);

            // Add settings provided in es Settings file
            URL elasticsearchSettingsPath = java.net.URI.create(configuration.getElasticsearchSettingsLocation()).toURL();
            LOGGER.debug("Using elasticsearch settings file: " + elasticsearchSettingsPath);
            ImmutableSettings.Builder esSettings = ImmutableSettings.builder().loadFromUrl(elasticsearchSettingsPath);
            launcher.addRuntimeSettings(esSettings);

            // Parse ports
            RunTimeSettings ports = new PortsModel(task);
            launcher.addRuntimeSettings(ports.getRuntimeSettings());

            // Parse ZooKeeper address
            RunTimeSettings zk = new ZooKeeperModel(configuration.getElasticsearchZKURL(), configuration.getElasticsearchZKTimeout());
            launcher.addRuntimeSettings(zk.getRuntimeSettings());

            // Parse cluster name
            launcher.addRuntimeSettings(ImmutableSettings.builder().put("cluster.name", configuration.getElasticsearchClusterName()));

            // Parse expected number of nodes
            launcher.addRuntimeSettings(ImmutableSettings.builder().put("gateway.expected_nodes", configuration.getElasticsearchNodes()));

            // Print final settings for logs.
            LOGGER.debug(launcher.toString());

            // Launch Node
            final Node node = launcher.launch();

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    taskStatus.setTaskState(taskID, Protos.TaskState.TASK_FINISHED, driver);
                    node.close();
                }
            }));

            taskStatus.setTaskState(taskID, Protos.TaskState.TASK_RUNNING, driver);
        } catch (InvalidParameterException e) {
            taskStatus.setTaskState(taskID, Protos.TaskState.TASK_FAILED, driver);
            LOGGER.error(e);
        }
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
        taskStatus.setTaskState(taskId, Protos.TaskState.TASK_FAILED, driver);
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        try {
            Protos.HealthCheck healthCheck = Protos.HealthCheck.parseFrom(data);
            LOGGER.info("HealthCheck request received: " + healthCheck.toString());
            driver.sendStatusUpdate(taskStatus.getTaskStatus());
        } catch (InvalidProtocolBufferException e) {
            LOGGER.debug("Unable to parse framework message as HealthCheck", e);
        }
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down executor...");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: " + message);
    }
}
