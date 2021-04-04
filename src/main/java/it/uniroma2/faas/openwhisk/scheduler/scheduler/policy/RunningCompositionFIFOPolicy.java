package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import com.google.common.base.Preconditions;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.*;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.advanced.ITraceable;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerPeriodicExecutors;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toUnmodifiableList;

class RunningCompositionFIFOPolicy implements IPolicy {

    public static final Policy POLICY = Policy.RUNNING_COMPOSITION_FIFO;

    public static final int THREAD_COUNT = 1;
    public static final long RUNNING_COMPOSITION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);
    public static final int RUNNING_COMPOSITIONS_LIMIT = 10;

    private final SchedulerPeriodicExecutors schedulerPeriodicExecutors =
            new SchedulerPeriodicExecutors(0, THREAD_COUNT);
    private final Object mutex = new Object();
    private long runningCompositionTimeLimitMs = RUNNING_COMPOSITION_TIME_LIMIT_MS;
    private int runningCompositionsLimit = RUNNING_COMPOSITIONS_LIMIT;
    // <causeID, timestamp> map
    private final Map<String, Long> runningCompositions = new HashMap<>(runningCompositionsLimit);

    public RunningCompositionFIFOPolicy() {
        throw new RuntimeException("Policy " + POLICY + " not yet implemented.");
    }

    @Override
    public @Nonnull Queue<ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        final Queue<ISchedulable> invocationQueue = new ArrayDeque<>(schedulables.size());
        if (schedulables.size() == 1) return new ArrayDeque<>(schedulables);

        // first, check if there are activations associated with compositions
        final Collection<ITraceable> compositionsActivations = schedulables.stream()
                .filter(ITraceable.class::isInstance)
                .map(ITraceable.class::cast)
                .filter(activation -> activation.getCause() != null)
                .collect(Collectors.toUnmodifiableList());
        // second, check if there are non-compositions activations
        schedulables.removeAll(compositionsActivations);
        if (compositionsActivations.isEmpty() || schedulables.isEmpty()) return invocationQueue;

        // TODO
        //   - create invocation queue and insert:
        //     first all traceable activations
        //     second all remaining traceable activations
        //     third all remaining non-traceable activations

        synchronized (mutex) {
            for (final ITraceable traceable : compositionsActivations) {
                if (runningCompositions.size() < runningCompositionsLimit) {
                    runningCompositions.put(traceable.getCause(), Instant.now().toEpochMilli());
                }

                if (runningCompositions.containsKey(traceable.getCause())) {
                    invocationQueue.add(traceable);
                }
            }
        }

        return invocationQueue;
    }

    @Override
    public void update(@Nonnull final Collection<? extends IConsumable> consumables) {
        final Collection<ActivationEvent> activationEvents = consumables.stream()
                .filter(ActivationEvent.class::isInstance)
                .map(ActivationEvent.class::cast)
                .filter(e -> e.getBody().getConductor())
                .collect(toUnmodifiableList());
        if (activationEvents.isEmpty()) return;

        synchronized (mutex) {
            for (final ActivationEvent activationEvent : activationEvents) {
                runningCompositions.remove(activationEvent.getBody().getActivationId());
            }
        }
    }

    private void removeOldEntries(long delta) {
        long now = Instant.now().toEpochMilli();
        synchronized (mutex) {
            runningCompositions.values().removeIf(timestamp -> now - timestamp > delta);
        }
    }

    private void schedulePeriodicActivities() {
        // remove entries for too old activations
        Objects.requireNonNull(schedulerPeriodicExecutors.computation()).scheduleWithFixedDelay(
                () -> removeOldEntries(runningCompositionTimeLimitMs),
                runningCompositionsLimit,
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