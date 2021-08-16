package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.AdvancedScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.RunningCompositionScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.BlockingCompletion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.Policy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.PolicyFactory;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerPeriodicExecutors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.ActivationKafkaConsumerMock.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.CompletionKafkaConsumerMock.COMPLETION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy.DEFAULT_PRIORITY;
import static java.util.stream.Collectors.toCollection;

public class RunningCompositionSchedulerMock extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(RunningCompositionScheduler.class.getCanonicalName());

    public static final int THREAD_COUNT = 1;
    public static final long RUNNING_COMPOSITION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(60);
    public static final int RUNNING_COMPOSITIONS_LIMIT = 10;
    public static final int MAX_BUFFER_SIZE = 1_000;

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
    private final Queue<Activation> compositionQueue = new ArrayDeque<>();
    // contains activation ID cause for max compositions allowed in the system
    private final HashMap<String, Integer> runningCompositionMap = new HashMap<>();
    // max buffer size
    private int maxBufferSize = MAX_BUFFER_SIZE;
    // policy
    private final IPolicy PQFIFO = PolicyFactory.createPolicy(Policy.PRIORITY_QUEUE_FIFO);

    public RunningCompositionSchedulerMock(final @Nonnull Scheduler scheduler) {
        super(scheduler);
        // scheduler periodic activities
        schedulePeriodicActivities();
    }

    @Override
    public void newEvent(final @Nonnull UUID stream, final @Nonnull Collection<?> data) {
        if (stream.equals(ACTIVATION_STREAM)) {
            final Queue<Activation> invocationQueue = new ArrayDeque<>();

            final Queue<Activation> simpleActivationQueue = data.stream()
                    .filter(Activation.class::isInstance)
                    .map(Activation.class::cast)
                    .collect(toCollection(ArrayDeque::new));
            final Queue<Activation> compositionActivationQueue = simpleActivationQueue.stream()
                    .filter(actv -> actv.getCause() != null)
                    .collect(toCollection(ArrayDeque::new));
            simpleActivationQueue.removeAll(compositionActivationQueue);

            invocationQueue.addAll(processCompositionActions(compositionActivationQueue));
            invocationQueue.addAll(simpleActivationQueue);

            if (!invocationQueue.isEmpty()) super.newEvent(ACTIVATION_STREAM, invocationQueue);
        } else if (stream.equals(COMPLETION_STREAM)) {
            final Queue<Activation> invocationQueue = new ArrayDeque<>();

            // composition activation are always *blocking* and have cause field defined
            final Queue<BlockingCompletion> compositionCompletions = data.stream()
                    .filter(BlockingCompletion.class::isInstance)
                    .map(BlockingCompletion.class::cast)
                    .filter(cmp -> cmp.getResponse().getCause() != null)
                    .collect(toCollection(ArrayDeque::new));
            invocationQueue.addAll(processCompositionCompletions(compositionCompletions));

            if (!data.isEmpty()) super.newEvent(COMPLETION_STREAM, data);
            if (!invocationQueue.isEmpty()) super.newEvent(ACTIVATION_STREAM, invocationQueue);
        }
    }

    private @Nonnull Queue<Activation> processCompositionActions(
            final @Nonnull Queue<Activation> compositionActivations) {
        final Queue<Activation> invocationQueue = new ArrayDeque<>(compositionActivations.size());
        if (compositionActivations.isEmpty()) return invocationQueue;

        synchronized (mutex) {
            final Queue<Activation> allowedActivations = checkCompositionLimit(compositionActivations);
            // buffering all activation not contained in invocation queue
            buffering(compositionActivations.stream()
                    .filter(actv -> !allowedActivations.contains(actv))
                    .collect(toCollection(ArrayDeque::new))
            );
            // update priority level for component activations, if any
            invocationQueue.addAll(traceCompositionPriority(allowedActivations));

            // log trace
            if (LOG.getLevel().equals(Level.TRACE)) policyStat();
        }

        return invocationQueue;
    }

    /**
     * TODO: manage errors
     *
     * @param blockingCompletions
     */
    private @Nonnull Queue<Activation> processCompositionCompletions(
            final @Nonnull Queue<BlockingCompletion> blockingCompletions) {
        // compositions generate only blocking activation (because of their dynamic nature)
        Queue<Activation> invocationQueue = new ArrayDeque<>(blockingCompletions.size());
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
                final Queue<Activation> allowedActivations = checkCompositionLimit(compositionQueue);
                compositionQueue.removeAll(allowedActivations);
                // update priority level for component activations, if any
                invocationQueue.addAll(traceCompositionPriority(allowedActivations));
                /*LOG.trace(String.format("[RCPQFIFO] Update - Scheduled %d previously buffered composition.",
                        invocationQueue.size()));*/
            }

            // log trace
            if (LOG.getLevel().equals(Level.TRACE)) policyStat();
        }

        return invocationQueue;
    }

    /**
     * Need external synchronization
     *
     * @param activations
     */
    private @Nonnull Queue<Activation> traceCompositionPriority(final Queue<Activation> activations) {
        final Queue<Activation> tracedPriority = new ArrayDeque<>(activations.size());
        for (final Activation activation : activations) {
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
    private @Nonnull Queue<Activation> checkCompositionLimit(final Queue<Activation> activations) {
        final Queue<Activation> invocationQueue = new ArrayDeque<>(activations.size());
        if (activations.isEmpty()) return invocationQueue;

        final Queue<? extends ISchedulable> sortedActivation = PQFIFO.apply(activations);
        while (!sortedActivation.isEmpty()) {
            final Activation activation = (Activation) sortedActivation.poll();
            final String cause = activation.getCause();
            // activation belongs to a currently running composition, so let it pass
            if (runningCompositionMap.containsKey(cause)) {
                // note that component activation could not be sorted by applied policy, but this ndoes not matter
                //   because they will be sorted by super.Scheduler
                invocationQueue.add(activation);
            } else if (runningCompositionMap.size() < runningCompositionsLimit) {
                Integer remainingComponentActivation = activation.getCmpLength();
                if (remainingComponentActivation == null) {
                    LOG.warn("[RCPQFIFO] Activation {} does not have cmpLength field and seems to belong to a composition (cause: {})",
                            activation.getActivationId(), cause);
                    remainingComponentActivation = 1;
                }
                // new composition registered in the system
                runningCompositionMap.put(cause, remainingComponentActivation);
                invocationQueue.add(activation);
            }
        }

        return invocationQueue;
    }

    private void buffering(final Queue<Activation> activations) {
        final int totalDemand = compositionQueue.size() + activations.size();
        if (totalDemand > maxBufferSize) {
            int toRemove = totalDemand - maxBufferSize;
            for (int i = 0; i < toRemove; ++i) ((ArrayDeque<Activation>) compositionQueue).removeLast();
            LOG.trace("[RCPQFIFO] Reached buffer limit ({}) - discarding last {} activations.",
                    maxBufferSize, toRemove);
        }

        compositionQueue.addAll(activations);
    }

    private void policyStat() {
        LOG.trace("[RCPQFIFO] Traced: {} - Running: {} - Queued: {}.",
                compositionPriorityMap.size(), runningCompositionMap.size(), compositionQueue.size());
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

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int size) {
        checkArgument(maxBufferSize > 0, "Max buffer size must be > 0.");
        maxBufferSize = size;
    }

}