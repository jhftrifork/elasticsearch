package org.apache.mesos.elasticsearch.scheduler.tasks.predicates;

import org.apache.mesos.Protos;

/**
 * Abstract predicate
 */
public interface OfferPredicate {
    String declineMessage(Protos.Offer offer);
    Boolean test(Protos.Offer offer);
}
