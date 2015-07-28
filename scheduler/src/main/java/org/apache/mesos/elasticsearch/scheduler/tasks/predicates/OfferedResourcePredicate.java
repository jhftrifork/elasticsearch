package org.apache.mesos.elasticsearch.scheduler.tasks.predicates;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.ResourceCheck;

/**
 * Tests to see whether the offer has the required resource
 */
public class OfferedResourcePredicate implements OfferPredicate {
    private final String resourceName;
    private final double requiredResource;

    public OfferedResourcePredicate(String resourceName, double requiredResource) {
        this.resourceName = resourceName;
        this.requiredResource = requiredResource;
    }

    @Override
    public String declineMessage(Protos.Offer offer) {
        return "Declined offer: Not enough " + resourceName + " resources";
    }

    @Override
    public Boolean test(Protos.Offer offer) {
        ResourceCheck resourceCheck = new ResourceCheck(resourceName);
        return resourceCheck.isEnough(offer.getResourcesList(), requiredResource);
    }
}
