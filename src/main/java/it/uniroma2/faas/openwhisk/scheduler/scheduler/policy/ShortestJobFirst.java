package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Action;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.BlockingCompletion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

/**
 * It is assumed that invoker's nodes are homogeneous, so that it is not necessary to group by
 * invoker and action but only by action.
 * This assumption was made due to scheduler-related design choices: {@link Scheduler} object
 * should be decoupled from selected {@link IPolicy}.
 * Otherwise, it would have been useful to group by invokers since, if invokers' nodes were heterogeneous,
 * probably they would have executed tha same action with noticeable difference.
 */
public class ShortestJobFirst implements IPolicy {

    public static final Policy POLICY = Policy.SHORTEST_JOB_FIRST;

    // OPTIMIZE: implement mechanism to prune too old actions
    // need external synchronization, in case of multiple threads
    private final Map<Action, Long> actionDurationMap = new HashMap<>();

    @Override
    public @Nonnull Queue<? extends ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        final Queue<ISchedulable> invocationQueue = new ArrayDeque<>(schedulables.size());
        if (schedulables.size() == 1) return invocationQueue;

        // group received schedulables by action
        final Map<Action, Collection<ISchedulable>> actionGroupedSchedulables = schedulables.stream()
                // see@ https://stackoverflow.com/questions/39172981/java-collectors-groupingby-is-list-ordered
                .collect(groupingBy(ISchedulable::getAction, toCollection(ArrayDeque::new)));

        // if there is no mapping for encountered action, add to map and set its duration as +inf
        actionGroupedSchedulables.keySet().forEach(as -> actionDurationMap.putIfAbsent(as, Long.MAX_VALUE));

        actionDurationMap.entrySet().stream()
                // sort from smallest to largest
                .sorted(Map.Entry.comparingByValue())
                .forEachOrdered(a -> {
                    if (actionGroupedSchedulables.containsKey(a.getKey()))
                        invocationQueue.addAll(actionGroupedSchedulables.get(a.getKey()));
                });

        return invocationQueue;
    }

    @Override
    public void update(@Nonnull final Collection<? extends Completion> completions) {
        // only blocking completions have "annotations" filed which contains action's "duration"
        final Collection<BlockingCompletion> blockingCompletions = completions.stream()
                .filter(BlockingCompletion.class::isInstance)
                .map(BlockingCompletion.class::cast)
                .filter(bc -> bc.getResponse() != null && bc.getResponse().getDuration() != null)
                .filter(bc -> bc.getResponse().getName() != null && bc.getResponse().getNamespace() != null &&
                        bc.getResponse().getVersion() != null)
                .collect(Collectors.toUnmodifiableList());

        for (final BlockingCompletion completion : blockingCompletions) {
            final Long newObservation = completion.getResponse().getDuration();
            final Action action = getActionFrom(completion);

            final Long currentEstimation = actionDurationMap.putIfAbsent(action, Long.MAX_VALUE);
            // set first value
            if (currentEstimation == null || currentEstimation == Long.MAX_VALUE) {
                actionDurationMap.put(action, newObservation);
            // update current estimation
            } else {
                // update estimate value for each completion received, without respect to target invoker
                actionDurationMap.put(action, estimate(currentEstimation, newObservation));
            }
        }
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

    /**
     * new_value = alpha * new_observation + (1 - alpha) * actual_estimation
     * alpha = 1 / 8
     * see@ https://it.wikipedia.org/wiki/Round_Trip_Time
     *
     * @param currentEstimation
     * @param newObservation
     * @return
     */
    private static long estimate(long currentEstimation, long newObservation) {
        final float alpha = 1f / 8f;
        return (long) (alpha * newObservation + (1f - alpha) * currentEstimation);
    }

    private static @Nonnull Action getActionFrom(@Nonnull final BlockingCompletion blockingCompletion) {
        return new Action(
                blockingCompletion.getResponse().getName(),
                blockingCompletion.getResponse().getNamespace(),
                blockingCompletion.getResponse().getVersion()
        );
    }

}