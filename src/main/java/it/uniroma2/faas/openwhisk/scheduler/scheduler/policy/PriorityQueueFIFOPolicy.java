package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.stream.Collectors.toCollection;

class PriorityQueueFIFOPolicy implements IPolicy {

    public static final Policy POLICY = Policy.PRIORITY_QUEUE_FIFO;

    // schedulable with higher priority goes first
    private final Comparator<ISchedulable> inversePriorityComparator = (s1, s2) ->
            Objects.requireNonNull(s2.getPriority()).compareTo(Objects.requireNonNull(s1.getPriority()));

    @Override
    public @Nonnull Queue<ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        if (schedulables.size() == 1) return new ArrayDeque<>(schedulables);
        return schedulables.stream()
                .filter(s -> s != null && s.getPriority() != null)
                .sorted(inversePriorityComparator)
                .collect(toCollection(ArrayDeque::new));
    }

    @Override
    public void update(final @Nonnull Collection<? extends IConsumable> consumables) {

    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

}