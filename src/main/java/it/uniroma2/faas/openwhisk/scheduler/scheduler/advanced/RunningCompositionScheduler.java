package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
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

    // TODO -
    //   - in completions sposta le composition con errore in un altra map così da non bloccare
    //   - esegui simulazione con tutte actv con cause 62539782acb3452f939782acb3552f9a
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
            final HashSet<String> compositionToRemove = creationMap.entrySet().stream()
                    .filter(entry -> now - entry.getValue() > delta)
                    .map(Map.Entry::getKey)
                    .collect(toCollection(HashSet::new));
            creationMap.keySet().removeAll(compositionToRemove);
            priorityMap.keySet().removeAll(compositionToRemove);
            remainingComponentActionMap.keySet().removeAll(compositionToRemove);
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

        public int getCapacity() {
            return limit;
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
            invocationQueue.addAll(checkCompositionLimit(compositionActivations));
            // buffering all activation not contained in invocation queue
            buffering(compositionActivations.stream()
                    .filter(actv -> !invocationQueue.contains(actv))
                    .collect(toCollection(ArrayDeque::new))
            );

            // log trace
            if (LOG.getLevel().equals(Level.TRACE)) policyStat("RCS|ACT");
        }

        return invocationQueue;
    }

    /**
     *
     * @param blockingCompletions
     */
    private @Nonnull Queue<Activation> processCompositionCompletions(
            final @Nonnull Queue<BlockingCompletion> blockingCompletions) {
        // compositions generate only blocking activation (because of their dynamic nature)
        final Queue<Activation> invocationQueue = new ArrayDeque<>(blockingCompletions.size());
        if (blockingCompletions.isEmpty()) return invocationQueue;

        synchronized (mutex) {
            for (final BlockingCompletion completion : blockingCompletions) {
                final String cause = completion.getResponse().getCause();

                if (runningCompositions.hasComposition(cause)) {
                    final int remainingComponentActivation = runningCompositions.getRemainingComponentAction(cause);
                    final int statusCode = completion.getResponse().getResult().getStatusCode();

                    // composition completed
                    if (remainingComponentActivation - 1 == 0) {
                        runningCompositions.removeComposition(cause);
                    // component action exited with an error, remove it when receiving secondary activation
                    // so set its remaining component activation to 1
                    } else if (statusCode > 0) {
                        runningCompositions.setRemainingComponentActionMap(cause, 1);
                    // reduce the number of remaining component activation needed to complete composition
                    } else {
                        runningCompositions.setRemainingComponentActionMap(cause, remainingComponentActivation - 1);
                    }
                // no entry found in currently running composition
                } else {
                    LOG.warn("[RCS] Received completion {} for a not traced composition (cause: {}).",
                            completion.getResponse().getActivationId(), cause);
                }
            }

            if (runningCompositions.hasCapacity() && !compositionQueue.isEmpty()) {
                invocationQueue.addAll(checkCompositionLimit(compositionQueue));
                compositionQueue.removeAll(invocationQueue);
                /*LOG.trace(String.format("[RCS] Update - Scheduled %d previously buffered composition.",
                        invocationQueue.size()));*/
            }

            // log trace
            if (LOG.getLevel().equals(Level.TRACE)) policyStat("RCS|CMP");
        }

        return invocationQueue;
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
            Integer priority = activation.getPriority();
            Integer remainingComponentActivation = activation.getCmpLength();
            if (priority == null) {
                LOG.trace("[RCS] Activation {} does not have priority field (cause: {}) - setting default ({}).",
                        activation.getActivationId(), cause, DEFAULT_PRIORITY);
                priority = DEFAULT_PRIORITY;
            }
            if (remainingComponentActivation == null) {
                LOG.trace("[RCS] Activation {} does not have cmpLength field (cause: {}) - setting default ({}).",
                        activation.getActivationId(), cause, 1);
                remainingComponentActivation = 1;
            }

            // activation belongs to a currently running composition, so let it pass
            // cause is always not null at this point
            if (runningCompositions.hasComposition(cause)) {
                // note that component activation could not be sorted by applied policy, but does not matter
                //   because they will be sorted by super.Scheduler
                final int tracedPriority = runningCompositions.getPriority(cause);
                // if priority does not match, create new object with correct priority
                if (tracedPriority != priority) {
                    /*LOG.trace("[RCS] Tracer - updating activation {} with priority {}.",
                            activation.getActivationId(), priorityFromCompositionMap);*/
                    invocationQueue.add(activation.with(tracedPriority));
                } else {
                    invocationQueue.add(activation);
                }
            } else if (runningCompositions.hasCapacity()) {
                // new composition registered in the system
                runningCompositions.addComposition(cause, priority, remainingComponentActivation);
                invocationQueue.add(activation);
            }
        }

        return invocationQueue;
    }

    private void buffering(final Queue<Activation> activations) {
        final int totalDemand = compositionQueue.size() + activations.size();
        if (totalDemand > maxBufferSize) {
            int toRemove = totalDemand - maxBufferSize;
            final Queue<Activation> sortedCompositionQueue = (Queue<Activation>) PQFIFO.apply(compositionQueue);
            final Queue<Activation> removedActivations = new ArrayDeque<>();
            for (int i = 0; i < toRemove; ++i)
                removedActivations.add(((ArrayDeque<Activation>) sortedCompositionQueue).removeLast());
            compositionQueue.removeAll(removedActivations);
            LOG.trace("[RCPQFIFO] Reached buffer limit ({}) - discarding last {} activations.",
                    maxBufferSize, toRemove);
        }

        compositionQueue.addAll(activations);
    }

    private void policyStat(final String tag) {
        LOG.trace("[{}] Running={} - Queue={}.",
                tag, runningCompositions.getCurrentSize(), compositionQueue.size());
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