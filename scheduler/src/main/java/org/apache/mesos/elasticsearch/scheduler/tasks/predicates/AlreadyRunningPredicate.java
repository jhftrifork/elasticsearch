package org.apache.mesos.elasticsearch.scheduler.tasks.predicates;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;

/**
 * Tests to see whether the task is already running
 */
public class AlreadyRunningPredicate implements OfferPredicate {
    private final ElasticsearchScheduler scheduler;

    public AlreadyRunningPredicate(ElasticsearchScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public String declineMessage(Protos.Offer offer) {
        return "Declined offer: Host " + offer.getHostname() + " is already running a task";
    }

    @Override
    public Boolean test(Protos.Offer offer) {
        return scheduler.getTasks().containsKey(offer.getId().getValue());
    }
}
