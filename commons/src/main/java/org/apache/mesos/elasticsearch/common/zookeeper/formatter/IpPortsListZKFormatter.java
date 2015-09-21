package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.exception.ZKAddressException;
import org.apache.mesos.elasticsearch.common.zookeeper.model.ZKAddress;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;

import java.util.List;

/**
 * Provides the ZooKeeper address(es) in a format required by Mesos.
 *
 * The format consists of a list of ZK servers
 *
 * Example: zk://host1:port1,host2:port2
 */
public class IpPortsListZKFormatter extends AbstractZKFormatter {

    public IpPortsListZKFormatter(ZKAddressParser parser) {
        super(parser);
    }

    /**
     * Get the ZooKeeper address, correctly formatted.
     * @param zkUrl The raw ZK address string in the format "zk://host:port/zkNode,..."
     * @throws ZKAddressException if the raw zkURL is invalid.
     * @return the zookeeper addresses in the format host:port[,host:port,...]
     */
    public String format(String zkUrl) {
        List<ZKAddress> addressList = parser.validateZkUrl(zkUrl);
        StringBuilder builder = new StringBuilder();
        addressList.forEach(add -> builder.append(",").append(add.getAddress()).append(":").append(add.getPort()));
        builder.deleteCharAt(0); // Delete first ','
        return builder.toString();
    }

}
