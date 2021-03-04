package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

public class PolicyFactory {

    public static @Nonnull IPolicy createPolicy(@Nonnull Policy policy) {
        checkNotNull(policy, "Policy can not be null.");

        switch (policy) {
            case PASS_THROUGH:
                return new PassThroughPolicy();

            case PRIORITY_QUEUE_FIFO:
                return new PriorityQueueFIFOPolicy();

            case RUNNING_COMPOSITION_FIFO:
                return new RunningCompositionFIFOPolicy();

            case SHORTEST_JOB_FIRST:
                return new ShortestJobFirst();

            default:
                throw new TypeNotPresentException(policy.name(), new Throwable("Selected policy not yet implemented."));
        }
    }

}