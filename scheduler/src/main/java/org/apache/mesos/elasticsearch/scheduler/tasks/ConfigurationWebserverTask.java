package org.apache.mesos.elasticsearch.scheduler.tasks;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.apache.mesos.elasticsearch.scheduler.Resources;
import org.apache.mesos.elasticsearch.scheduler.tasks.predicates.AlreadyRunningPredicate;
import org.apache.mesos.elasticsearch.scheduler.tasks.predicates.OfferContainsPortsPredicate;
import org.apache.mesos.elasticsearch.scheduler.tasks.predicates.OfferPredicate;
import org.apache.mesos.elasticsearch.scheduler.tasks.predicates.OfferedResourcePredicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Generates taskinfo for webserver executor.
 */
public class ConfigurationWebserverTask extends ExecutorTask {

    public static final int N_REQUIRED_PORTS = 1;
    public static final double CPU_RESOURCE = 0.1;
    public static final int RAM_RESOURCE = 64;
    public static final int DISK_RESOURCE = 0;
    private final ElasticsearchScheduler scheduler;

    public ConfigurationWebserverTask(ElasticsearchScheduler scheduler) {
        super();
        this.scheduler = scheduler;
    }

    @Override
    protected Collection<OfferPredicate> populatePredicates() {
        List<OfferPredicate> predicateList = new ArrayList<>();
        predicateList.add(new AlreadyRunningPredicate(scheduler));
        predicateList.add(new OfferContainsPortsPredicate(N_REQUIRED_PORTS));
        predicateList.add(new OfferedResourcePredicate(Resources.cpus(0).getName(), CPU_RESOURCE));
        predicateList.add(new OfferedResourcePredicate(Resources.mem(0).getName(), RAM_RESOURCE));
        predicateList.add(new OfferedResourcePredicate(Resources.disk(0).getName(), DISK_RESOURCE));
        ...
        return predicateList;
    }

    @Override
    public Protos.TaskInfo create() {
        return null;
    }

    @Override
    public void log(String str) {

    }
}
