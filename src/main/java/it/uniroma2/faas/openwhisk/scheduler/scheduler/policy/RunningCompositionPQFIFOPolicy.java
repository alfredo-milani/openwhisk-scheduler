package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.BlockingCompletion;
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
import java.util.stream.Collectors;

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
    private final HashMap<String, Integer> runningCompositionMap = new HashMap<>();

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
            // TODO - allow only max N composition in the queue (delete older ones)
            compositionQueue.addAll(compositionActivations);
            // update priority level for component activations, if any
            invocationQueue = traceCompositionPriority(invocationQueue);

            // log trace
            if (LOG.getLevel().equals(Level.TRACE)) {
                LOG.trace("[RCPQFIFO] Traced: {} - Running: {} - Queued: {}.",
                        compositionPriorityMap.size(), runningCompositionMap.size(), compositionQueue.size());
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
        final Collection<BlockingCompletion> blockingCompletions = consumables.stream()
                .filter(BlockingCompletion.class::isInstance)
                .map(BlockingCompletion.class::cast)
                .filter(bc -> bc.getResponse() != null)
                .collect(Collectors.toUnmodifiableList());
        Queue<ISchedulable> invocationQueue = new ArrayDeque<>(consumables.size());
        if (blockingCompletions.isEmpty()) return invocationQueue;

        synchronized (mutex) {
            for (final BlockingCompletion completion : blockingCompletions) {
                final String cause = completion.getResponse().getCause();
                final Integer remainingComponentActivation = runningCompositionMap.getOrDefault(cause, -1);

                // no entry found in currently running composition
                if (remainingComponentActivation < 0) {
                    LOG.warn("Received completion {} for a non-traced composition (cause: {}).",
                            completion.getResponse().getActivationId(), cause);
                // composition completed
                } else if (remainingComponentActivation - 1 == 0) {
                    runningCompositionMap.remove(cause);
                    compositionPriorityMap.remove(cause);
                // reduce the number of remaining component activation needed to complete composition
                } else {
                    runningCompositionMap.put(cause, remainingComponentActivation - 1);
                }
            }

            if (runningCompositionMap.size() < runningCompositionsLimit && !compositionQueue.isEmpty()) {
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
                        compositionPriorityMap.size(), runningCompositionMap.size(), compositionQueue.size());
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
            final String cause = activation.getCause();
            // activation belongs to a currently running composition, so let it pass
            if (runningCompositionMap.containsKey(cause)) {
                invocationQueue.add(activation);
            } else if (runningCompositionMap.size() < runningCompositionsLimit) {
                Integer remainingComponentActivation = activation.getCmpLength();
                if (remainingComponentActivation == null) {
                    LOG.warn("[RCPQFIFO] Activation {} does not have cmpLength field and seems to belong to a composition (cause: {})",
                            activation.getActivationId(), cause);
                    remainingComponentActivation = 1;
                }
                runningCompositionMap.put(cause, remainingComponentActivation);
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
            compositionPriorityMap.forEach((cause, priorityTimestampEntry) -> {
                long timestamp = priorityTimestampEntry.getValue();
                if (now - timestamp > delta) {
                    toRemove.add(cause);
                }
            });
            runningCompositionMap.keySet().removeAll(toRemove);
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