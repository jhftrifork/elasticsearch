package org.apache.mesos.elasticsearch.scheduler.tasks;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.scheduler.Clock;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.Resources;
import org.apache.mesos.elasticsearch.scheduler.TaskInfoFactory;
import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ConfigurationWebserverTaskInfoFactory {
    private static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);
    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";
    Clock clock = new Clock();

    /**
     * Creates TaskInfo for Webserver execcutor running in a Docker container
     *
     * @param offer with resources to run the executor with
     * @return TaskInfo
     */
    public Protos.TaskInfo createWebserverTask(Configuration configuration, Protos.Offer offer) {
        List<Integer> ports = Resources.selectOnePortsFromRange(offer.getResourcesList());

        List<Protos.Resource> acceptedResources = new ArrayList<Protos.Resource>();
        acceptedResources.add(Resources.cpus(0.1));
        acceptedResources.add(Resources.mem(64));
        acceptedResources.add(Resources.disk(0));

        LOGGER.info("Creating Webserver task [port: " + ports.get(0) + "]");

        Protos.DiscoveryInfo.Builder discovery = Protos.DiscoveryInfo.newBuilder();
        Protos.Ports.Builder discoveryPorts = Protos.Ports.newBuilder();
        acceptedResources.add(Resources.singlePortRange(ports.get(0)));
        discoveryPorts.addPorts(Discovery.WEBSERVER_PORT_INDEX, Protos.Port.newBuilder().setNumber(ports.get(0)).setName(Discovery.WEBSERVER_PORT_NAME));
        discovery.setPorts(discoveryPorts);
        discovery.setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL);

        return Protos.TaskInfo.newBuilder()
                .setName("ConfigurationWebserver")
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId(offer)))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(acceptedResources)
                .setDiscovery(discovery)
                .setExecutor(executorInfo(configuration, acceptedResources)).build();
    }

    Protos.ExecutorInfo.Builder executorInfo(Configuration configuration, List<Protos.Resource> resources) {
        return Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setFrameworkId(configuration.getFrameworkId())
                .setName("elasticsearch-webserver-" + UUID.randomUUID().toString())
                .addAllResources(resources)
                .setCommand(newWebserverCommandInfo(configuration))
                .setContainer(Protos.ContainerInfo.newBuilder()
                        .setType(Protos.ContainerInfo.Type.DOCKER)
                        .setDocker(Protos.ContainerInfo.DockerInfo.newBuilder().setImage("mesos/elasticsearch-configuration"))
                        .build());
    }

    Protos.CommandInfo.Builder newWebserverCommandInfo(Configuration configuration) {
        ExecutorEnvironmentalVariables executorEnvironmentalVariables = new ExecutorEnvironmentalVariables(configuration);
        return Protos.CommandInfo.newBuilder()
                .setShell(false)
                .setEnvironment(Protos.Environment.newBuilder().addAllVariables(executorEnvironmentalVariables.getList()))
                .setContainer(Protos.CommandInfo.ContainerInfo.newBuilder().setImage("mesos/elasticsearch-configuration").build());
    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format("elasticsearch_%s_%s", offer.getHostname(), date);
    }
}