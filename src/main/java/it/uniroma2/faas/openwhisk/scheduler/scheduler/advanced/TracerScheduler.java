package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.ITraceable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;

/**
 * This class add functionality to base scheduler, tracing all actions generated
 * (from Apache OpenWhisk Controller component) and assigning to them priority respectively.
 * To all subsequent actions will be assigned priority based on first action in composition.
 */
public class TracerScheduler extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(TracerScheduler.class.getCanonicalName());

    public static final long DEFAULT_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);

    private long timeLimitMs = DEFAULT_TIME_LIMIT_MS;
    // see@ https://stackoverflow.com/questions/14148331/how-to-get-a-hashmap-value-with-three-values
    // represent the state of currently active compositions - <PrimaryActivationID, <Priority, Timestamp>>
    private final Map<String, Map.Entry<Integer, Long>> compositionsMap = new HashMap<>();

    public TracerScheduler(@Nonnull Scheduler scheduler) {
        super(scheduler);
    }

    /**
     * Action belonging to a composition are traced based on {@link Activation#getCause()} field
     * in order to track priority. This is necessary because subsequent actions in a composition are generated
     * by OpenWhisk Controller component directly and could not have {@link Activation#K_SCHEDULER} field
     * associated with them.
     *
     * In this first implementation, actions are associated with a temporal mark and are removed from the map
     * once are considered too old, based on {@link #timeLimitMs}. Note that using timeouts can led to consider
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
            long traceablesCount = dataList.stream()
                    .filter(ITraceable.class::isInstance)
                    .count();
            LOG.trace("Traceables objects: {}, over {} received on stream {}.",
                    traceablesCount, data.size(), stream.toString());
            if (traceablesCount > 0) {
                traceCompositions(dataList);
            }
        }
        removeOldEntries(timeLimitMs);

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
            Object datum = listIterator.next();
            if (datum instanceof ITraceable) {
                ITraceable traceable = (ITraceable) datum;
                // if cause is not null current activation belongs to a composition
                if (traceable.getCause() != null) {
                    int priority = traceable.getPriority() == null
                            ? 0
                            : traceable.getPriority();
                    Map.Entry<Integer, Long> entry = compositionsMap.putIfAbsent(
                            traceable.getActivationId(),
                            new AbstractMap.SimpleImmutableEntry<>(priority, Instant.now().toEpochMilli())
                    );
                    // if there is already an entry for PrimaryActivationID, use priority associated to process activation
                    if (entry != null) {
                        // if traced priority differs, create new traceable with correct priority
                        if (entry.getKey() != priority) {
                            LOG.trace("Adding priority level {} associated with cause {} for activation with id {}",
                                    entry.getKey(), traceable.getCause(), traceable.getActivationId());
                            listIterator.set(traceable.with(entry.getKey()));
                        }
                    } else {
                        LOG.trace("Created new entry for cause id {} with priority {} - generated by activation with id {}.",
                                traceable.getCause(), traceable.getPriority(), traceable.getActivationId());
                    }
                }
                // otherwise probably the activation received does not belongs to a composition
            }
        }
    }

    /**
     * Remove entries older than {@link #timeLimitMs}.
     */
    private void removeOldEntries(long delta) {
        LOG.trace("Updating compositions map.");
        long now = Instant.now().toEpochMilli();
        int sizeBeforeUpdate = compositionsMap.size();
        compositionsMap.values().removeIf(entry -> now - entry.getValue() > delta);
        int sizeAfterUpdate = compositionsMap.size();
        if (sizeBeforeUpdate != sizeAfterUpdate) {
            LOG.trace("Removed {} elements from compositions map (retention {} ms).",
                    sizeBeforeUpdate - sizeAfterUpdate, delta);
        }
    }

    public void setTimeLimitMs(long timeLimitMs) {
        checkArgument(timeLimitMs >= 0, "Time limit must be >= 0.");
        this.timeLimitMs = timeLimitMs;
    }

}