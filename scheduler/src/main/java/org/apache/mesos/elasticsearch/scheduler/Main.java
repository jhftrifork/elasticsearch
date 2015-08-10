package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.*;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableZookeeperState;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Application which starts the Elasticsearch scheduler
 */
public class Main {

    public static final String NUMBER_OF_HARDWARE_NODES_OPTION_NAME = "n";
    public static final String ZK_URL_OPTION_NAME = "zk";
    public static final String MANAGEMENT_API_PORT_OPTION_NAME = "m";
    public static final String RAM_OPTION_NAME = "ram";
    public static final String FRAMEWORK_NAME_OPTION_NAME = "fn";

    public static final long ZK_TIMEOUT = 20000L;
    public static final String CLUSTER_NAME = "/mesos-ha";
    public static final String FRAMEWORK_NAME = "/elasticsearch-mesos";

    private Options options;

    private Configuration configuration;

    public Main() {
        this.options = new Options();
        this.options.addOption(NUMBER_OF_HARDWARE_NODES_OPTION_NAME, "numHardwareNodes", true, "number of hardware nodes");
        this.options.addOption(ZK_URL_OPTION_NAME, "zookeeperUrl", true, "Zookeeper urls in the format zk://IP:PORT,IP:PORT,...)");
        this.options.addOption(MANAGEMENT_API_PORT_OPTION_NAME, "StatusPort", true, "TCP port for status interface. Default is 8080");
        this.options.addOption(RAM_OPTION_NAME, "ElasticsearchRam", true, "Amount of RAM to give the Elasticsearch instances");
        this.options.addOption(FRAMEWORK_NAME_OPTION_NAME, "frameworkName", true, "Name to give to the Mesos framework instance");
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.run(args);
    }

    public void run(String[] args) {
        checkEnv();

        try {
            parseCommandlineOptions(args);
        } catch (ParseException | IllegalArgumentException e) {
            printUsageAndExit();
            return;
        }

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(configuration, new TaskInfoFactory());

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("server.port", String.valueOf(configuration.getManagementApiPort()));
        new SpringApplicationBuilder(WebApplication.class)
                .properties(properties)
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("scheduler", scheduler))
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("configuration", configuration))
                .showBanner(false)
                .run(args);

        scheduler.run();
    }

    private void checkEnv() {
        Map<String, String> env = System.getenv();
        checkHeap(env.get(ExecutorEnvironmentalVariables.JAVA_OPTS));
    }

    private void checkHeap(String s) {
        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("Scheduler heap space not set!");
        }
    }

    private void parseCommandlineOptions(String[] args) throws ParseException, IllegalArgumentException {
        configuration = new Configuration();

        DefaultParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String numberOfHwNodesString = cmd.getOptionValue(NUMBER_OF_HARDWARE_NODES_OPTION_NAME);
        String zkUrl = cmd.getOptionValue(ZK_URL_OPTION_NAME);
        String ram = cmd.getOptionValue(RAM_OPTION_NAME, Double.toString(configuration.getMem()));
        String managementApiPort = cmd.getOptionValue(MANAGEMENT_API_PORT_OPTION_NAME, "8080");
        String frameworkName = cmd.getOptionValue(FRAMEWORK_NAME_OPTION_NAME, "elasticsearch");

        if (numberOfHwNodesString == null || zkUrl == null) {
            printUsageAndExit();
            return;
        }

        configuration.setZookeeperUrl(getMesosZKURL(zkUrl));
        configuration.setVersion(getClass().getPackage().getImplementationVersion());
        configuration.setNumberOfHwNodes(Integer.parseInt(numberOfHwNodesString));
        configuration.setState(getState(zkUrl));
        configuration.setMem(Double.parseDouble(ram));
        configuration.setManagementApiPort(Integer.parseInt(managementApiPort));
        configuration.setFrameworkName(frameworkName);
    }

    private SerializableState getState(String zkUrl) {
        org.apache.mesos.state.State state = new ZooKeeperState(
                getMesosStateZKURL(zkUrl),
                ZK_TIMEOUT,
                TimeUnit.MILLISECONDS,
                FRAMEWORK_NAME + CLUSTER_NAME);
        return  new SerializableZookeeperState(state);
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
    }

    private String getMesosZKURL(String zkUrl) {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(zkUrl);
    }

    private void printUsageAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(configuration.getFrameworkName(), options);
        System.exit(2);
    }

}
