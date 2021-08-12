package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ActivationEvent;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerPeriodicExecutors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toCollection;

// TODO: This implementation considers only compositions, so activation
//  without 'cause' field will be not considered
//  (testHealthAction are not managed by policy by default)
public class RunningCompositionPQFIFOPolicy extends PriorityQueueFIFOPolicy {

    private final static Logger LOG = LogManager.getLogger(RunningCompositionPQFIFOPolicy.class.getCanonicalName());

    public static final Policy POLICY = Policy.RUNNING_COMPOSITION_PQFIFO;

    public static final int THREAD_COUNT = 1;
    public static final long RUNNING_COMPOSITION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(30);
    public static final int RUNNING_COMPOSITIONS_LIMIT = 10;

    private final SchedulerPeriodicExecutors schedulerPeriodicExecutors =
            new SchedulerPeriodicExecutors(0, THREAD_COUNT);
    private final Object mutex = new Object();
    private long runningCompositionTimeLimitMs = RUNNING_COMPOSITION_TIME_LIMIT_MS;
    private int runningCompositionsLimit = RUNNING_COMPOSITIONS_LIMIT;
    // see@ https://stackoverflow.com/questions/14148331/how-to-get-a-hashmap-value-with-three-values
    // represent the state of currently active compositions - <PrimaryActivationID, <Priority, Timestamp>>
    // using ConcurrentHashMap is sufficient to ensure correctness even if there are two threads operating on it
    private final Map<String, Map.Entry<Integer, Long>> compositionPriorityMap = new HashMap<>();
    // buffer containing all not scheduled composition activations
    private final Queue<ISchedulable> compositionQueue = new ArrayDeque<>();
    // contains activation ID cause for max compositions allowed in the system
    private final HashSet<String> runningCompositionSet = new HashSet<>();
    // schedulable with higher priority goes first, i.e., with highest priority value (integer)
    private final Comparator<ISchedulable> inversePriorityComparator = (s1, s2) ->
            Objects.requireNonNull(s2.getPriority()).compareTo(Objects.requireNonNull(s1.getPriority()));

    public RunningCompositionPQFIFOPolicy() {
        schedulePeriodicActivities();
    }

    @Override
    public @Nonnull Queue<ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        Queue<ISchedulable> invocationQueue = new ArrayDeque<>(schedulables.size());

        Queue<ISchedulable> compositionActivations = schedulables.stream()
                .filter(Activation.class::isInstance)
                .map(Activation.class::cast)
                // TODO: manage also simple activation not generated from composition action
                .filter(actv -> actv.getCause() != null)
                .collect(toCollection(ArrayDeque::new));

        if (compositionActivations.size() != schedulables.size()) {
            LOG.warn(String.format("[RCPQFIFO] Apply - Received %s activation - Processed %s activation.",
                    schedulables.size(), compositionActivations.size()));
        }

        if (compositionActivations.isEmpty()) return invocationQueue;

        synchronized (mutex) {
            // ensure that composition limit is preserved, enqueue all other compositions
            invocationQueue.addAll(checkCompositionLimit(compositionActivations));
            compositionActivations.removeAll(invocationQueue);
            compositionQueue.addAll(compositionActivations);
            // update priority level for component activations, if any
            invocationQueue = traceCompositionPriority(invocationQueue);

            // log trace
            if (LOG.getLevel().equals(Level.TRACE)) {
                LOG.trace("[RCPQFIFO] Traced: {} - Running: {} - Queued: {}.",
                        compositionPriorityMap.size(), runningCompositionSet.size(), compositionQueue.size());
            }
        }

        return invocationQueue;
    }

    /**
     * Need external synchronization
     *
     * @param consumables collection of {@link IConsumable} needed to updated policy' state.
     * @return
     */
    @Override
    public Queue<? extends IConsumable> update(@Nonnull final Collection<? extends IConsumable> consumables) {
        final Queue<ActivationEvent> activationEvents = consumables.stream()
                .filter(ActivationEvent.class::isInstance)
                .map(ActivationEvent.class::cast)
                .filter(e -> e.getBody().getConductor())
                .collect(toCollection(ArrayDeque::new));
        if (activationEvents.isEmpty()) return activationEvents;

        Queue<ISchedulable> invocationQueue = new ArrayDeque<>(consumables.size());
        synchronized (mutex) {
            for (final ActivationEvent activationEvent : activationEvents) {
                final String activationId = activationEvent.getBody().getActivationId();
                compositionPriorityMap.remove(activationId);
                runningCompositionSet.remove(activationId);
                /*LOG.trace(String.format("[RCPQFIFO] Update - removing composition %s - current size %s",
                        activationId, compositionPriorityMap.size()));*/
            }

            if (runningCompositionSet.size() < runningCompositionsLimit && !compositionQueue.isEmpty()) {
                invocationQueue.addAll(checkCompositionLimit(compositionQueue));
                compositionQueue.removeAll(invocationQueue);
                // update priority level for component activations, if any
                invocationQueue = traceCompositionPriority(invocationQueue);
                /*LOG.trace(String.format("[RCPQFIFO] Update - Scheduled %d previously buffered composition.",
                        invocationQueue.size()));*/
            }

            // log trace
            if (LOG.getLevel().equals(Level.TRACE)) {
                LOG.trace("[RCPQFIFO] Traced: {} - Running: {} - Queued: {}.",
                        compositionPriorityMap.size(), runningCompositionSet.size(), compositionQueue.size());
            }
        }

        return invocationQueue;
    }

    /**
     * Need external synchronization
     *
     * @param activations
     */
    private @Nonnull Queue<ISchedulable> traceCompositionPriority(final Queue<ISchedulable> activations) {
        final Queue<ISchedulable> tracedPriority = new ArrayDeque<>(activations.size());
        for (final ISchedulable schedulable : activations) {
            final Activation activation = (Activation) schedulable;
            final String cause = activation.getCause();
            // probably the activation received does not belong to a composition
//            if (cause == null) continue;

            final int priority = activation.getPriority() == null
                    ? DEFAULT_PRIORITY
                    : activation.getPriority();
            final Map.Entry<Integer, Long> priorityTimestampEntry = compositionPriorityMap.get(cause);
            // create new entry
            if (priorityTimestampEntry == null) {
                compositionPriorityMap.put(
                        cause,
                        new AbstractMap.SimpleImmutableEntry<>(priority, Instant.now().toEpochMilli())
                );
                /*LOG.trace("[RCPQFIFO] Tracer - new cause {} registered with priority {} (actual size: {}).",
                        cause, priority, compositionPriorityMap.size());*/
                tracedPriority.add(activation);
            // check if current activation has wrong priority
            } else {
                final Integer priorityFromCompositionMap = priorityTimestampEntry.getKey();
                // if priority does not match, create new object with correct priority
                if (priorityFromCompositionMap != priority) {
                    /*LOG.trace("[RCPQFIFO] Tracer - updating activation {} with priority {}.",
                            activation.getActivationId(), priorityFromCompositionMap);*/
                    tracedPriority.add(activation.with(priorityFromCompositionMap));
                } else {
                    tracedPriority.add(activation);
                }
            }
        }

        return tracedPriority;
    }

    /**
     * Need external synchronization
     *
     * @param activations
     * @return
     */
    private @Nonnull Queue<ISchedulable> checkCompositionLimit(final Queue<ISchedulable> activations) {
        final Queue<ISchedulable> invocationQueue = new ArrayDeque<>(activations.size());
        if (activations.isEmpty()) return invocationQueue;

        final Queue<ISchedulable> sortedActivation = super.apply(activations);
        while (!sortedActivation.isEmpty()) {
            final Activation activation = (Activation) sortedActivation.poll();
            // activation belongs to a currently running composition, so let it pass
            if (runningCompositionSet.contains(activation.getCause())) {
                invocationQueue.add(activation);
            } else if (runningCompositionSet.size() < runningCompositionsLimit) {
                runningCompositionSet.add(activation.getCause());
                invocationQueue.add(activation);
            }
        }

        return invocationQueue;
    }

    /**
     * Thread-safe
     *
     * @param delta
     */
    private void removeOldEntries(long delta) {
        long now = Instant.now().toEpochMilli();
        synchronized (mutex) {
            final int sizeBeforeUpdate = compositionPriorityMap.size();
            final Collection<String> toRemove = new ArrayDeque<>();
            compositionPriorityMap.forEach((key, value) -> {
                long timestamp = value.getValue();
                if (now - timestamp > delta) {
                    toRemove.add(key);
                }
            });
            runningCompositionSet.removeAll(toRemove);
            compositionPriorityMap.keySet().removeAll(toRemove);
            final int sizeAfterUpdate = compositionPriorityMap.size();
            if (sizeBeforeUpdate > sizeAfterUpdate) {
                LOG.trace("[RCPQFIFO] Pruning - Removed {} activations from compositions map (actual size: {}) - time delta {} ms.",
                        sizeBeforeUpdate - sizeAfterUpdate, sizeAfterUpdate, delta);
            }
        }
    }

    private void schedulePeriodicActivities() {
        // remove entries for too old activations
        Objects.requireNonNull(schedulerPeriodicExecutors.computation()).scheduleWithFixedDelay(
                () -> removeOldEntries(runningCompositionTimeLimitMs),
                0L,
                runningCompositionTimeLimitMs,
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

    public void setRunningCompositionTimeLimitMs(long runningCompositionTimeLimitMs) {
        checkArgument(runningCompositionTimeLimitMs >= 0, "Time limit must be >= 0.");
        this.runningCompositionTimeLimitMs = runningCompositionTimeLimitMs;
    }

    public long getRunningCompositionTimeLimitMs() {
        return runningCompositionTimeLimitMs;
    }

    public int getRunningCompositionsLimit() {
        return runningCompositionsLimit;
    }

    public void setRunningCompositionsLimit(int runningCompositionsLimit) {
        checkArgument(runningCompositionsLimit > 0, "Running compositions limit must be > 0.");
        this.runningCompositionsLimit = runningCompositionsLimit;
    }

}