package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Action;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.BlockingCompletion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
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
    // queue containing all activation ids to update
    private final Queue<String> activationIdQueue = new ArrayDeque<>();

    @Override
    public @Nonnull Queue<? extends ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        final Queue<ISchedulable> invocationQueue = new ArrayDeque<>(schedulables.size());
        if (schedulables.size() == 1) return new ArrayDeque<>(schedulables);

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

    /* TODO - implement method to update action's execution time from CouchDB
                to deal even with non-blocking calls
    @Override
    public void update(@Nonnull final Collection<? extends IConsumable> consumables) {
        final Collection<Completion> completions = consumables.stream()
                .filter(Completion.class::isInstance)
                .map(Completion.class::cast)
                .collect(Collectors.toUnmodifiableList());

        for (final Completion completion : completions) {
            final String activationId;
            if (completion instanceof NonBlockingCompletion) {
                activationId = ((NonBlockingCompletion) completion).getActivationId();
            } else if (completion instanceof BlockingCompletion) {
                activationId = ((BlockingCompletion) completion).getResponse().getActivationId();
            } else {
                activationId = null;
            }

            if (activationId == null || activationId.isEmpty() || activationId.isBlank()) continue;
            activationIdQueue.add(activationId);
        }



        for (final BlockingCompletion completion : blockingCompletions) {
            final Long newObservation = completion.getResponse().getDuration();
            final Action action = getActionFrom(completion);
            // update current estimation
            actionDurationMap.merge(action, newObservation, ShortestJobFirst::estimate);
        }
    }*/

    @Override
    public void update(@Nonnull final Collection<? extends IConsumable> consumables) {
        // only blocking completions have "annotations" field which contains action's "duration"
        // NOTE: to use ShortestJobFirst policy even with non-blocking action, must be implemented
        //   new Kafka consumer for topic "events" and must be enabled "user_events" in deploy configuration
        //   see@ https://github.com/apache/openwhisk/blob/master/docs/metrics.md#user-specific-metrics
        final Collection<BlockingCompletion> blockingCompletions = consumables.stream()
                .filter(BlockingCompletion.class::isInstance)
                .map(BlockingCompletion.class::cast)
                .filter(bc -> bc.getResponse() != null)
                .collect(Collectors.toUnmodifiableList());

        for (final BlockingCompletion completion : blockingCompletions) {
            final long newObservation = getMetricFrom(completion, false);
            final Action action = getActionFrom(completion);
            // update current estimation
            actionDurationMap.merge(action, newObservation, ShortestJobFirst::estimate);
        }
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

    /**
     * New method to retrieve effective duration, since field "duration" includes "initTime".
     *
     * @param blockingCompletion
     * @param includeInitTime
     * @return
     */
    private static long getMetricFrom(@Nonnull final BlockingCompletion blockingCompletion,
                                      final boolean includeInitTime) {
        Long duration = blockingCompletion.getResponse().getDuration();
        if (duration == null) return 0L;

        if (!includeInitTime) {
            final List<Map<String, Object>> annotations = blockingCompletion.getResponse().getAnnotations();
            for (final Map<String, Object> annotation : annotations) {
                if (annotation.get("key").equals("initTime"))
                    duration -= (int) annotation.get("value");
            }

        }

        return duration;
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