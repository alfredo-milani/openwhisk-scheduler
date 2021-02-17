package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;


import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.AdvancedScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.TracerScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ActivationEvent;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.advanced.ITraceable;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerPeriodicExecutors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.ActivationKafkaConsumerMock.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.EventKafkaConsumerMock.EVENT_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy.DEFAULT_PRIORITY;
import static java.util.stream.Collectors.toCollection;

/**
 * This class add functionality to base scheduler, tracing all actions generated
 * (from Apache OpenWhisk Controller component) and assigning to them priority respectively.
 * To all subsequent actions will be assigned priority based on first action in composition.
 */
public class TracerSchedulerMock extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(TracerScheduler.class.getCanonicalName());

    public static final int THREAD_COUNT = 1;
    public static final long RUNNING_COMPOSITION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);

    private final SchedulerPeriodicExecutors schedulerPeriodicExecutors = new SchedulerPeriodicExecutors(0, THREAD_COUNT);
    private final Object mutex = new Object();
    private long runningCompositionTimeLimitMs = RUNNING_COMPOSITION_TIME_LIMIT_MS;
    // see@ https://stackoverflow.com/questions/14148331/how-to-get-a-hashmap-value-with-three-values
    // represent the state of currently active compositions - <PrimaryActivationID, <Priority, Timestamp>>
    // using ConcurrentHashMap is sufficient to ensure correctness even if there are two threads operating on it
    private final Map<String, Map.Entry<Integer, Long>> compositionsMap = new ConcurrentHashMap<>();

    public TracerSchedulerMock(@Nonnull Scheduler scheduler) {
        super(scheduler);

        // scheduler periodic activities
        schedulePeriodicActivities();
    }

    /**
     * Action belonging to a composition are traced based on {@link Activation#getCause()} field
     * in order to track priority. This is necessary because subsequent actions in a composition are generated
     * by OpenWhisk Controller component directly and could not have {@link Activation#K_SCHEDULER} field
     * associated with them.
     *
     * In this first implementation, actions are associated with a temporal mark and are removed from the map
     * once are considered too old, based on {@link #runningCompositionTimeLimitMs}. Note that using timeouts can led to consider
     * activation with high priority to be processed as activation with default priority.
     *
     * In newer implementation could be considered to use Kafka topic "completedN" to remove completed compositions.
     * @param stream stream indicator
     * @param data consumable object to be processed
     */
    @Override
    public void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data) {
        checkNotNull(stream, "Stream can not be null.");
        checkNotNull(data, "Data can not be null.");

        // using wildcard, without filtering ISchedulable objects, to maintain all
        //   objects types in the list
        // will be traced only ITraceable objects
        final List<?> dataList = new ArrayList<>(data);
        if (stream.equals(ACTIVATION_STREAM)) {
            // counting traceable objects
            final long traceablesCount = dataList.stream()
                    .filter(ITraceable.class::isInstance)
                    .count();
            LOG.trace("[TRC] - Processing {} traceables objects (over {} received).",
                    traceablesCount, data.size());

            if (traceablesCount > 0) {
                final long activationsCountFromComposition = dataList.stream()
                        .filter(ITraceable.class::isInstance)
                        .map(ITraceable.class::cast)
                        .filter(t -> t.getCause() != null)
                        .count();
                LOG.trace("Received {} objects associated with compositions.",
                        activationsCountFromComposition);
                synchronized (mutex) {
                    traceCompositions(dataList);
                }
            }
        } else if (stream.equals(EVENT_STREAM)) {
            final Collection<ActivationEvent> activationEvents = data.stream()
                    .filter(ActivationEvent.class::isInstance)
                    .map(ActivationEvent.class::cast)
                    .filter(e -> e.getBody().getConductor())
                    .filter(e -> e.getBody().getActivationId() != null)
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[CMP] - Processing {} completion objects (over {} received).",
                    activationEvents.size(), data.size());

            if (activationEvents.size() > 0) {
                synchronized (mutex) {
                    final int sizeBeforeUpdate = compositionsMap.size();
                    activationEvents.forEach(e -> compositionsMap.remove(e.getBody().getActivationId()));
                    final int sizeAfterUpdate = compositionsMap.size();
                    if (sizeBeforeUpdate > sizeAfterUpdate) {
                        LOG.trace("Removed {} activations from compositions map (actual size: {}).",
                                sizeBeforeUpdate - sizeAfterUpdate, sizeAfterUpdate);
                    }
                }
            }
            return;
        }

        super.newEvent(stream, dataList);
    }

    /**
     * In place substitution of {@link ITraceable} objects with missing priority level.
     * Only {@link ITraceable} objects will be considered. Non-{@link ITraceable} objects will not
     * be modified.
     *
     * @param data schedulables objects.
     */
    private void traceCompositions(@Nonnull final List<?> data) {
        checkNotNull(data, "Data can not be null.");

        // traceable objects are not filtered before to maintain an order list
        ListIterator<?> listIterator = data.listIterator();
        while (listIterator.hasNext()) {
            final Object datum = listIterator.next();
            if (datum instanceof ITraceable) {
                final ITraceable traceable = (ITraceable) datum;
                // if cause is not null current activation belongs to a composition
                if (traceable.getCause() != null) {
                    final int traceablePriority = traceable.getPriority() == null
                            ? DEFAULT_PRIORITY
                            : traceable.getPriority();
                    final Map.Entry<Integer, Long> priorityTimestampEntry =
                            compositionsMap.get(traceable.getCause());
                    // create new entry
                    if (priorityTimestampEntry == null) {
                        compositionsMap.put(
                                traceable.getCause(),
                                new AbstractMap.SimpleImmutableEntry<>(traceablePriority, Instant.now().toEpochMilli())
                        );
                        LOG.trace("Registered new primary activation with id {} - priority {}.",
                                traceable.getCause(), traceablePriority);
                        // check if current activation has wrong priority
                    } else {
                        final Integer priorityFromCompositionMap = priorityTimestampEntry.getKey();
                        // if priority does not match, create new object with correct priority
                        if (priorityFromCompositionMap != traceablePriority) {
                            LOG.trace("Updating to priority level {} associated with cause {}, for activation with id {}",
                                    priorityFromCompositionMap, traceable.getCause(), traceable.getActivationId());
                            listIterator.set(traceable.with(priorityFromCompositionMap));
                        }
                    }
                }
                // otherwise probably the activation received does not belongs to a composition
            }
        }
    }

    /**
     * Remove entries older than {@link #runningCompositionTimeLimitMs}.
     */
    private void removeOldEntries(long delta) {
        checkArgument(delta >= 0, "Delta time must be >= 0.");
        final long now = Instant.now().toEpochMilli();
        synchronized (mutex) {
            final int sizeBeforeUpdate = compositionsMap.size();
            compositionsMap.values().removeIf(entry -> now - entry.getValue() > delta);
            final int sizeAfterUpdate = compositionsMap.size();
            if (sizeBeforeUpdate > sizeAfterUpdate) {
                LOG.trace("Removed {} activations from compositions map (actual size: {}) - time delta {} ms.",
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

}
