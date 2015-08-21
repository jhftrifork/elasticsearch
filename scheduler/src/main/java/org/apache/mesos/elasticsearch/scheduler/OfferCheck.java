package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

import java.util.List;

/**
 * A predicate over Proto.Offer values.
 */
@FunctionalInterface
public interface OfferCheck {
    /**
     * @return A list of human-readable reasons why the offer is inadequate.
     */
    public List<String> checkOffer(Protos.Offer offer);
}
