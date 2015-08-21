package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

import java.util.Arrays;
import java.util.List;

/**
 * A predicate over Proto.Offer values which checks a single numeric resource in the offer is great enough to run an ES task.
 */
public class ResourceOfferCheck implements OfferCheck {
    private final String resourceName;
    private final double minimumValue;
    private final String failureString;

    /**
     * @param resourceName the name of the resource you want to check
     */
    public ResourceOfferCheck(String resourceName, double minimumValue, String failureString) {
        this.resourceName = resourceName;
        this.minimumValue = minimumValue;
        this.failureString = failureString;
    }

    public List<String> checkOffer(Protos.Offer offer) {
        Protos.Resource resource = getResource(offer.getResourcesList());
        if (resource.getScalar().getValue() >= minimumValue) {
            return Arrays.asList();
        } else {
            return Arrays.asList(failureString + " (requires " + minimumValue + " but offer only has " + resource.getScalar().getValue() + ")");
        }
    }

    private Protos.Resource getResource(List<Protos.Resource> resourcesList) {
        for (Protos.Resource resource : resourcesList) {
            if (resource.getName().equals(resourceName)) {
                return resource;
            }
        }
        return null;
    }
}