package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

class PriorityQueueFIFOPolicy implements IPolicy {

    private final static Logger LOG = LogManager.getLogger(PriorityQueueFIFOPolicy.class.getCanonicalName());

    public static final Policy POLICY = Policy.PRIORITY_QUEUE_FIFO;
    public static final int DEFAULT_PRIORITY = 0;

    @Override
    public @Nonnull Queue<ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        checkNotNull(schedulables, "Consumables can not be null.");

        // higher priority to activations with higher priority level (priority level represented as integer)
        // TreeMap default order is ascending, so here is used Comparator provided from Collections to get reverse order
        final SortedMap<Integer, Queue<ISchedulable>> prioritiesQueuesMap = new TreeMap<>(Collections.reverseOrder());
        // creates new entry if there is not yet a queue associated with new priority level received
        for (ISchedulable schedulable : schedulables) {
            Integer priority = schedulable.getPriority();
            if (priority == null) {
                LOG.warn("Priority for schedulable with id {} is null; using default value (0).",
                        schedulable.getActivationId());
                schedulable = schedulable.with(DEFAULT_PRIORITY);
                priority = schedulable.getPriority();
            }
            if (prioritiesQueuesMap.putIfAbsent(priority, new ArrayDeque<>()) == null) {
                LOG.trace("Created new activation queue for priority {}.", priority);
            }
            prioritiesQueuesMap.get(priority).add(schedulable);
        }

        int elements = prioritiesQueuesMap.values().stream().mapToInt(Collection::size).sum();
        final Queue<ISchedulable> priorityQueue = new ArrayDeque<>(elements);
        // TreeMap#values, using Collections#reverseOrder provides queues in descending order, so
        //   queue associated with higher priority level are processed first
        for (Queue<ISchedulable> pq : prioritiesQueuesMap.values()) {
            priorityQueue.addAll(pq);
        }
        return priorityQueue;
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

}