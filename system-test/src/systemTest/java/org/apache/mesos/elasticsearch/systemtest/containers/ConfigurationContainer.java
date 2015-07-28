package org.apache.mesos.elasticsearch.systemtest.containers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.mini.container.AbstractContainer;

/**
 * Container for configuration image
 */
public class ConfigurationContainer extends AbstractContainer {

    public static final String IMAGE_NAME = "mesos/elasticsearch-configuration";
    public static final String TAG = "latest";

    public ConfigurationContainer(DockerClient dockerClient) {
        super(dockerClient);
    }

    @Override
    protected void pullImage() {
        pullImage(IMAGE_NAME, TAG);
    }

    @Override
    protected CreateContainerCmd dockerCommand() {
        return dockerClient
                .createContainerCmd(IMAGE_NAME + ":" + TAG);
//                .withExposedPorts(ExposedPort.parse("" + proxyPort))
//                .withPortBindings(PortBinding.parse("0.0.0.0:" + proxyPort + ":8888"));
    }
}
