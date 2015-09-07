package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.scheduler.cluster.ClusterMonitor;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.stream.Collectors;

/**
 * Scheduler for Elasticsearch.
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ElasticsearchScheduler implements Scheduler {

    private static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    private final Configuration configuration;

    private final TaskInfoFactory taskInfoFactory;

    private ClusterMonitor clusterMonitor = null;

    Clock clock = new Clock();

    Map<String, Task> tasks = new HashMap<>();
    private Observable statusUpdateWatchers = new StatusUpdateObservable();
    private Boolean registered = false;

    public ElasticsearchScheduler(Configuration configuration, TaskInfoFactory taskInfoFactory) {
        this.configuration = configuration;
        this.taskInfoFactory = taskInfoFactory;
    }

    public Map<String, Task> getTasks() {
        return tasks;
    }

    public void run() {
        LOGGER.info("Starting ElasticSearch on Mesos - [numHwNodes: " + configuration.getElasticsearchNodes() +
                    ", zk mesos: " + configuration.getMesosZKURL() +
                    ", zk framework: " + configuration.getFrameworkZKURL() +
                    ", ram:" + configuration.getMem() + "]");

        FrameworkInfoFactory frameworkInfoFactory = new FrameworkInfoFactory(configuration);
        final Protos.FrameworkInfo.Builder frameworkBuilder = frameworkInfoFactory.getBuilder();

        final MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkBuilder.build(), configuration.getMesosZKURL());

        driver.run();
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        FrameworkState frameworkState = new FrameworkState(configuration.getState());
        frameworkState.setFrameworkId(frameworkId);
        configuration.setFrameworkState(frameworkState); // DCOS certification 02

        LOGGER.info("Framework registered as " + frameworkId.getValue());

        ClusterState clusterState = new ClusterState(configuration.getState(), frameworkState); // Must use new framework state. This is when we are allocated our FrameworkID.
        clusterMonitor = new ClusterMonitor(configuration, this, driver, clusterState);
        statusUpdateWatchers.addObserver(clusterMonitor);

        List<Protos.Resource> resources = Resources.buildFrameworkResources(configuration);

        Protos.Request request = Protos.Request.newBuilder()
                .addAllResources(resources)
                .build();

        List<Protos.Request> requests = Collections.singletonList(request);
        driver.requestResources(requests);
        registered = true;
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Framework re-registered");
    }

    @SuppressWarnings({"PMD.NPathComplexity"})
    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        if (!registered) {
            LOGGER.debug("Not registered, can't accept resource offers.");
            return;
        }

        List<OfferCheck> checks = Arrays.asList(
                (Protos.Offer offer) ->
                        isHostAlreadyRunningTask(offer)
                                ? Arrays.asList("Host " + offer.getHostname() + " is already running an Elasticsearch task")
                                : Arrays.asList(),
                (Protos.Offer offer) ->
                        clusterMonitor.getClusterState().getTaskList().size() == configuration.getElasticsearchNodes()
                                ? Arrays.asList("Mesos is already running " + configuration.getElasticsearchNodes() + " Elasticsearch tasks")
                                : Arrays.asList(),
                (Protos.Offer offer) ->
                        !containsEnoughPorts(offer.getResourcesList())
                                ? Arrays.asList("Offer did not contain 2 ports for Elasticsearch client and transport connection")
                                : Arrays.asList(),
                new ResourceOfferCheck(Resources.cpus(0).getName(), configuration.getCpus(), "Not enough CPU"),
                new ResourceOfferCheck(Resources.mem(0).getName(), configuration.getMem(), "Not enough RAM"),
                new ResourceOfferCheck(Resources.disk(0).getName(), configuration.getDisk(), "Not enough disk")
        );

        for (Protos.Offer offer : offers) {
            LOGGER.info("Considering offer: " + offer.toString());

            List<String> failures = checks.stream().flatMap((OfferCheck pred) -> pred.checkOffer(offer).stream()).collect(Collectors.toList());

            if (failures.isEmpty()) {
                acceptOffer(driver, offer);
            } else {
                declineOffer(driver, offer, String.join("; ", failures));
            }
        }
    }

    private void declineOffer(SchedulerDriver driver, Protos.Offer offer, String reason) {
        LOGGER.info("Declining offer because: " + reason);
        driver.declineOffer(offer.getId());
    }

    private void acceptOffer(SchedulerDriver driver, Protos.Offer offer) {
        LOGGER.info("Accepting Mesos offer for host with hostname: " + offer.getHostname());
        Protos.TaskInfo taskInfo = taskInfoFactory.createTask(configuration, offer);
        LOGGER.debug("Launching Mesos task: " + taskInfo.toString());
        driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
        Task task = new Task(
                offer.getHostname(),
                taskInfo.getTaskId().getValue(),
                Protos.TaskState.TASK_STAGING,
                clock.zonedNow(),
                new InetSocketAddress(offer.getHostname(), taskInfo.getDiscovery().getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()),
                new InetSocketAddress(offer.getHostname(), taskInfo.getDiscovery().getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber())
        );
        tasks.put(taskInfo.getTaskId().getValue(), task);
        clusterMonitor.monitorTask(taskInfo); // Add task to cluster monitor
    }

    private boolean containsEnoughPorts(List<Protos.Resource> resources) {
        int count = Resources.selectTwoPortsFromRange(resources).size();
        return count == 2;
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOGGER.info("Offer " + offerId.getValue() + " rescinded");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info("Status update - Task with ID '" + status.getTaskId().getValue() + "' is now in state '" + status.getState() + "'. Message: " + status.getMessage());
        statusUpdateWatchers.notifyObservers(status);
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        LOGGER.info("Framework Message - Executor: " + executorId.getValue() + ", SlaveID: " + slaveId.getValue());
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOGGER.warn("Disconnected");
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        LOGGER.info("Slave lost: " + slaveId.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        // This is never called by Mesos, so we have to call it ourselves via a healthcheck
        // https://issues.apache.org/jira/browse/MESOS-313
        LOGGER.info("Executor lost: " + executorId.getValue() +
                "on slave " + slaveId.getValue() +
                "with status " + status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("Error: " + message);
    }

    private boolean isHostAlreadyRunningTask(Protos.Offer offer) {
        Boolean result = false;
        List<Protos.TaskInfo> stateList = clusterMonitor.getClusterState().getTaskList();
        for (Protos.TaskInfo t : stateList) {
            if (t.getSlaveId().equals(offer.getSlaveId())) {
                result = true;
            }
        }
        return result;
    }

    /**
     * Implementation of Observable to fix the setChanged problem.
     */
    private static class StatusUpdateObservable extends Observable {
        @Override
        public void notifyObservers(Object arg) {
            this.setChanged(); // This is ridiculous.
            super.notifyObservers(arg);
        }
    }
}
