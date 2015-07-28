package org.apache.mesos.elasticsearch.scheduler.tasks;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.tasks.predicates.OfferPredicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Abstract class representing a task for an executor.
 */
public abstract class ExecutorTask {
    private static final Logger LOGGER = Logger.getLogger(ExecutorTask.class);
    protected final List<OfferPredicate> predicateList = new ArrayList<>();

    protected ExecutorTask() {
        predicateList.addAll(populatePredicates());
    }

    public Boolean isAccepted(Protos.Offer offer) {
        if (predicateList.isEmpty()) {
            LOGGER.warn("Predicate list is empty");
        }
        Boolean accept = true;
        for (OfferPredicate p : predicateList) {
            if (!p.test(offer)) {
                log(p.declineMessage(offer));
                accept = false;
            }
        }
        return accept;
    }

    protected abstract Collection<OfferPredicate> populatePredicates();
    public abstract Protos.TaskInfo create();
    public abstract void log(String str);
}
