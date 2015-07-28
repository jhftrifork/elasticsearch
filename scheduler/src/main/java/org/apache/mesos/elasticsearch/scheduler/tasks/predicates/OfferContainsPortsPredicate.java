package org.apache.mesos.elasticsearch.scheduler.tasks.predicates;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.apache.mesos.elasticsearch.scheduler.Resources;

/**
 * Tests to see if there are ports available
 */
public class OfferContainsPortsPredicate implements OfferPredicate {
    private final Integer nRequiredPorts;

    public OfferContainsPortsPredicate(Integer nRequiredPorts) {
        this.nRequiredPorts = nRequiredPorts;
    }

    @Override
    public String declineMessage(Protos.Offer offer) {
        return "Declined offer: Offer did not contain " + nRequiredPorts + " ports";
    }

    @Override
    public Boolean test(Protos.Offer offer) {
        int count = Resources.selectOnePortsFromRange(offer.getResourcesList()).size();
        return count == nRequiredPorts;
    }
}
