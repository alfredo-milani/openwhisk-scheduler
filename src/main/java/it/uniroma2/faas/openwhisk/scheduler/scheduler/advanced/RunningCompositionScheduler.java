package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.BlockingCompletion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.Policy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.PolicyFactory;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerPeriodicExecutors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer.COMPLETION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy.DEFAULT_PRIORITY;
import static java.util.stream.Collectors.toCollection;

public class RunningCompositionScheduler extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(RunningCompositionScheduler.class.getCanonicalName());

    public static final int THREAD_COUNT = 1;
    public static final long RUNNING_COMPOSITION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(60);
    public static final int MAX_BUFFER_SIZE = 1_000;

    private final SchedulerPeriodicExecutors schedulerPeriodicExecutors =
            new SchedulerPeriodicExecutors(0, THREAD_COUNT);
    private final Object mutex = new Object();
    private long runningCompositionTimeLimitMs = RUNNING_COMPOSITION_TIME_LIMIT_MS;
    // buffer containing all not scheduled composition activations
    private final Queue<Activation> compositionQueue = new ArrayDeque<>();
    // object containing info about running compositions
    private final RunningComposition runningCompositions;
    // object containing info about failed compositions
    private final RunningComposition failedCompositions;
    // max buffer size
    private int maxBufferSize = MAX_BUFFER_SIZE;
    // policy
    private final IPolicy PQFIFO = PolicyFactory.createPolicy(Policy.PRIORITY_QUEUE_FIFO);

    private static class RunningComposition {
        public static final int UNTRACKED_PRIORITY = Integer.MIN_VALUE;
        public static final int UNTRACKED_RCA = Integer.MIN_VALUE;

        private int limit;
        private final HashMap<String, Long> creationMap = new HashMap<>();
        private final HashMap<String, Integer> priorityMap = new HashMap<>();
        private final HashMap<String, Integer> remainingComponentActionMap = new HashMap<>();

        RunningComposition() {
            this(Integer.MAX_VALUE);
        }

        RunningComposition(final int limit) {
            checkArgument(limit > 0, "Limit must be > 0.");
            this.limit = limit;
        }

        public int getLimit() {
            return limit;
        }

        public void setLimit(int limit) {
            checkArgument(limit > 0, "Limit must be > 0.");
            this.limit = limit;
        }

        public boolean addComposition(final @Nonnull String cause) {
            return addComposition(cause, UNTRACKED_PRIORITY, UNTRACKED_RCA);
        }

        public boolean addComposition(final @Nonnull String cause, final int priority,
                                      final int remainingComponentAction) {
            if (!hasCapacity()) return false;
            creationMap.put(cause, Instant.now().toEpochMilli());
            priorityMap.put(cause, priority);
            remainingComponentActionMap.put(cause, remainingComponentAction);
            return true;
        }

        public void removeComposition(final @Nonnull String cause) {
            creationMap.remove(cause);
            priorityMap.remove(cause);
            remainingComponentActionMap.remove(cause);
        }

        public void pruneOlderThan(final long delta) {
            checkArgument(delta > 0, "Delta must be > 0.");
            final long now = Instant.now().toEpochMilli();
            creationMap.entrySet().stream()
                    .filter(entry -> now - entry.getValue() > delta)
                    .map(Map.Entry::getKey)
                    .collect(toCollection(HashSet::new))
                    .forEach(cause -> {
                        creationMap.remove(cause);
                        priorityMap.remove(cause);
                        remainingComponentActionMap.remove(cause);
                    });
        }

        public int getPriority(final @Nonnull String cause) {
            return priorityMap.getOrDefault(cause, UNTRACKED_PRIORITY);
        }

        public int getRemainingComponentAction(final @Nonnull String cause) {
            return remainingComponentActionMap.getOrDefault(cause, UNTRACKED_RCA);
        }

        public void setPriority(final @Nonnull String cause, final int newPriority) {
            checkArgument(creationMap.containsKey(cause),
                    "No composition found with cause {}.", cause);
            priorityMap.put(cause, newPriority);
        }

        public void setRemainingComponentActionMap(final @Nonnull String cause, final int remainingComponentAction) {
            checkArgument(creationMap.containsKey(cause),
                    "No composition found with cause {}.", cause);
            remainingComponentActionMap.put(cause, remainingComponentAction);
        }

        public int getCurrentSize() {
            return creationMap.size();
        }

        public boolean hasComposition(final @Nonnull String cause) {
            return creationMap.containsKey(cause);
        }

        public boolean hasCapacity() {
            return creationMap.size() < limit;
        }

        @Override
        public String toString() {
            return "RunningComposition{" +
                    "limit=" + limit +
                    ", creationMapSize=" + creationMap.size() +
                    ", priorityMapSize=" + priorityMap.size() +
                    ", remainingComponentActionMapSize=" + remainingComponentActionMap.size() +
                    '}';
        }
    }

    public RunningCompositionScheduler(final @Nonnull Scheduler scheduler) {
        super(scheduler);
        this.runningCompositions = new RunningComposition();
        this.failedCompositions = new RunningComposition();
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
                    .filter(actv -> actv.getCause() == null)
                    .collect(toCollection(ArrayDeque::new));
            final Queue<Activation> compositionActivationQueue = data.stream()
                    .filter(Activation.class::isInstance)
                    .map(Activation.class::cast)
                    .filter(actv -> actv.getCause() != null)
                    .collect(toCollection(ArrayDeque::new));

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
            invocationQueue.addAll(checkCompositionLimit(compositionActivations));
            // buffering all activation not contained in invocation queue
            buffering(compositionActivations.stream()
                    .filter(actv -> !invocationQueue.contains(actv))
                    .collect(toCollection(ArrayDeque::new))
            );

            LOG.trace("[RCS|ACT] Running={} - Queue={} - Failed={}.",
                    runningCompositions.getCurrentSize(), compositionQueue.size(), failedCompositions.getCurrentSize());
        }

        return invocationQueue;
    }

    /**
     * Need external synchronization
     *
     * @param blockingCompletions
     */
    private @Nonnull Queue<Activation> processCompositionCompletions(
            final @Nonnull Queue<BlockingCompletion> blockingCompletions) {
        // compositions generate only blocking activation (because of their dynamic nature)
        final Queue<Activation> invocationQueue = new ArrayDeque<>(blockingCompletions.size());
        if (blockingCompletions.isEmpty()) return invocationQueue;

        synchronized (mutex) {
            updateRemainingComponentAction(blockingCompletions);

            if (runningCompositions.hasCapacity() && !compositionQueue.isEmpty()) {
                invocationQueue.addAll(checkCompositionLimit(compositionQueue));
                compositionQueue.removeAll(invocationQueue);
            }

            LOG.trace("[RCS|CMP] Running={} - Queue={} - Failed={}.",
                    runningCompositions.getCurrentSize(), compositionQueue.size(), failedCompositions.getCurrentSize());
        }

        return invocationQueue;
    }

    private void updateRemainingComponentAction(final @Nonnull Queue<BlockingCompletion> blockingCompletions) {
        for (final BlockingCompletion completion : blockingCompletions) {
            final String cause = completion.getResponse().getCause();

            if (runningCompositions.hasComposition(cause)) {
                final int remainingComponentActivation = runningCompositions.getRemainingComponentAction(cause);
                final int statusCode = completion.getResponse().getResult().getStatusCode();

                // composition completed
                if (remainingComponentActivation - 1 == 0) {
                    runningCompositions.removeComposition(cause);
//                    LOG.trace("[RCS|CMP] Removing composition {}", cause);
                // component action exited with an error, remove it when receiving secondary activation
                // so set its remaining component activation to 1
                } else if (statusCode > 0) {
//                    runningCompositions.setRemainingComponentActionMap(cause, 1);
                    runningCompositions.removeComposition(cause);
                    failedCompositions.addComposition(cause);
                    LOG.trace("[RCS|CMP] Composition {} return an error (statusCode={}).", cause, statusCode);
                // reduce the number of remaining component activation needed to complete composition
                } else {
                    runningCompositions.setRemainingComponentActionMap(cause, remainingComponentActivation - 1);
                }
            } else if (failedCompositions.hasComposition(cause)) {
                LOG.trace("[RCS|CMP] Received failed composition {} - cause {}.",
                        completion.getResponse().getActivationId(), completion.getResponse().getCause());
            // no entry found in currently running composition
            } else {
                LOG.warn("[RCS] Received completion {} for a not traced composition (cause: {}).",
                        completion.getResponse().getActivationId(), cause);
            }
        }
    }

    private @Nonnull Queue<Activation> traceActivations(final @Nonnull Queue<Activation> activations) {
        final Queue<Activation> tracedActivations = new ArrayDeque<>(activations.size());
        if (activations.isEmpty()) return tracedActivations;

        for (final Activation activation : activations) {
            final String cause = activation.getCause();
            Activation tracedActivation = activation;
            Integer priority = activation.getPriority();
            Integer remainingComponentActivation = activation.getCmpLength();
            if (priority == null) {
                LOG.trace("[RCS] Activation {} does not have priority field (cause: {}) - setting default ({}).",
                        activation.getActivationId(), cause, DEFAULT_PRIORITY);
                priority = DEFAULT_PRIORITY;
                tracedActivation = tracedActivation.with(priority);
            }
            if (remainingComponentActivation == null) {
                LOG.trace("[RCS] Activation {} does not have cmpLength field (cause: {}) - setting default ({}).",
                        activation.getActivationId(), cause, 1);
                remainingComponentActivation = 1;
                tracedActivation = tracedActivation.withCmpLength(remainingComponentActivation);
            }

            // cause is always not null at this point
            if (runningCompositions.hasComposition(cause)) {
                final int tracedPriority = runningCompositions.getPriority(cause);
                final int tracedRCA = runningCompositions.getRemainingComponentAction(cause);
                // if priority does not match, create new object with correct priority
                if (tracedPriority != priority) {
                    tracedActivation = tracedActivation.with(tracedPriority);
                    LOG.trace("[RCS] Tracer - updating activation {} ({}) with priority {}.",
                            activation.getActivationId(), cause, tracedPriority);
                }
                if (tracedRCA != remainingComponentActivation) {
                    tracedActivation = tracedActivation.withCmpLength(tracedRCA);
                    LOG.trace("[RCS] Tracer - updating activation {} ({}) with cmpLength {}.",
                            activation.getActivationId(), cause, tracedRCA);
                }
            }
            tracedActivations.add(tracedActivation);
        }

        return tracedActivations;
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

        final Queue<? extends ISchedulable> sortedActivation = PQFIFO.apply(traceActivations(activations));
        while (!sortedActivation.isEmpty()) {
            final Activation activation = (Activation) sortedActivation.poll();
            final String cause = activation.getCause();
            // these values can not be null because they are tuned in traceActivation method
            final int priority = activation.getPriority();
            final int remainingComponentActivation = activation.getCmpLength();

            // activation belongs to a currently running composition, so let it pass
            // cause is always not null at this point
            if (runningCompositions.hasComposition(cause) || failedCompositions.hasComposition(cause)) {
                invocationQueue.add(activation);
            } else if (runningCompositions.hasCapacity()) {
                // new composition (secondary activation) registered in the system
                runningCompositions.addComposition(cause, priority, remainingComponentActivation);
                invocationQueue.add(activation);
            }
        }

        return invocationQueue;
    }

    /**
     * Need external synchronization
     *
     * @param activations
     */
    private void buffering(final Queue<Activation> activations) {
        if (activations.isEmpty()) return;
        if (activations.size() > maxBufferSize) {
            LOG.warn("[RCS] Too many activations received - rejecting {} activations.",
                    activations.size() - maxBufferSize);
        }

        final int totalDemand = compositionQueue.size() + activations.size();
        if (totalDemand > maxBufferSize) {
            final int toRemove = totalDemand - maxBufferSize;
            // NOTE: inside compositionQueue should only be composition activation
            //   so it is not necessary to trace them before apply the policy
            PQFIFO.apply(compositionQueue).stream()
                    .skip(Math.max(0L, (long) compositionQueue.size() - toRemove))
                    .forEach(compositionQueue::remove);
            LOG.trace("[RCS] Reached buffer limit ({}) - discarding last {} activations.",
                    maxBufferSize, toRemove);
        }

        // allow at most maxBufferSize activation into buffer at a time
        int maxBufferedActivations = maxBufferSize;
        for (final Activation activation : activations) {
            if (maxBufferedActivations-- > 0) compositionQueue.add(activation);
        }
    }

    /**
     * Thread-safe
     *
     * @param delta
     */
    private void removeOldEntries(long delta) {
        synchronized (mutex) {
            final int sizeBeforeUpdate = runningCompositions.getCurrentSize();
            runningCompositions.pruneOlderThan(delta);
            failedCompositions.pruneOlderThan(delta * 2);
            final int sizeAfterUpdate = runningCompositions.getCurrentSize();
            if (sizeBeforeUpdate > sizeAfterUpdate) {
                LOG.trace("[RCS] Pruning - Removed {} activations from compositions map (actual size: {}) - time delta {} ms.",
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
        return runningCompositions.getLimit();
    }

    public void setRunningCompositionsLimit(int runningCompositionsLimit) {
        checkArgument(runningCompositionsLimit > 0, "Running compositions limit must be > 0.");
        this.runningCompositions.setLimit(runningCompositionsLimit);
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int size) {
        checkArgument(maxBufferSize > 0, "Max buffer size must be > 0.");
        maxBufferSize = size;
    }

}