package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Health;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.HealthKafkaConsumer.HEALTH_STREAM;

/**
 * Decorator for {@link Scheduler} contract that let {@link ISchedulable} to pass through only if
 * there is a healthy invoker that can process the request. All other {@link IConsumable} objects are not considered
 * and will be sent forward.
 */
public class HealthCheckerScheduler extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(HealthCheckerScheduler.class.getCanonicalName());

    public static final long DEFAULT_HEALTHY_CHECKS = TimeUnit.SECONDS.toMillis(10);
    public static final long DEFAULT_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);

    private long healthyChecks = DEFAULT_HEALTHY_CHECKS;
    private long timeLimitMs = DEFAULT_TIME_LIMIT_MS;
    //
    private final Map<String, Map.Entry<State, Long>> invokersHealth = new ConcurrentHashMap<>(24);
    //
    private final Map<String, ConcurrentLinkedDeque<ISchedulable>> invokersHealthQueue = new HashMap<>(24);

    private enum State {
        HEALTHY,
        UNHEALTHY,
        OFFLINE
    }

    public HealthCheckerScheduler(@Nonnull Scheduler scheduler) {
        super(scheduler);
    }

    @Override
    public void newEvent(@Nonnull UUID stream, @Nonnull Collection<?> data) {
        if (stream.equals(HEALTH_STREAM)) {
            // TODO: manage case when an invoker get updated with more/less memory
            final Collection<? extends Health> heartbeats = data.stream()
                    .filter(Health.class::isInstance)
                    .map(Health.class::cast)
                    .distinct()
                    .collect(Collectors.toCollection(ArrayDeque::new));
            LOG.trace("Processing {} unique hearthbeats objects (over {} received).",
                    heartbeats.size(), data.size());
            if (heartbeats.size() > 0) {
                for (final Health health : heartbeats) {
                    final String invokerTarget = health.getInstance().getInstanceType().getName() +
                            health.getInstance().getInstance();
                    final Map.Entry<State, Long> invokerState = invokersHealth.getOrDefault(
                            invokerTarget,
                            new AbstractMap.SimpleEntry<>(State.OFFLINE, Instant.now().toEpochMilli())
                    );
                    if (!invokerState.getKey().equals(State.HEALTHY)) {
                        invokersHealth.put(
                                invokerTarget,
                                new AbstractMap.SimpleEntry<>(State.HEALTHY, Instant.now().toEpochMilli())
                        );
                        // invoker's state has changed, so check if there are queued schedulables
                        ConcurrentLinkedDeque<ISchedulable> invokerQueue =
                                invokersHealthQueue.get(invokerTarget);
                        if (invokerQueue != null) {
                            // get snapshot of this queue
                            invokerQueue = new ConcurrentLinkedDeque<>(invokerQueue);
                            if (invokerQueue.size() > 0) {
                                super.newEvent(ACTIVATION_STREAM, List.of(invokerQueue));
                                final ConcurrentLinkedDeque<ISchedulable> currentQueue =
                                        invokersHealthQueue.get(invokerTarget);
                                if (currentQueue != null)
                                    currentQueue.removeAll(invokerQueue);
                            }
                        }
                    } else {
                        invokerState.setValue(Instant.now().toEpochMilli());
                        invokersHealth.put(
                                invokerTarget,
                                invokerState
                        );
                    }
                }
            }
        } else {
            final Collection<IConsumable> consumables = data.stream()
                    .filter(IConsumable.class::isInstance)
                    .map(IConsumable.class::cast)
                    .collect(Collectors.toCollection(ArrayDeque::new));
            LOG.trace("Processing {} consumables (over {} received).", consumables.size(), data.size());

            final Collection<IConsumable> invocationQueue = new ArrayDeque<>(consumables.size());
            for (IConsumable consumable : consumables) {
                if (consumable instanceof ISchedulable) {
                    final ISchedulable schedulable = (ISchedulable) consumables;
                    final String invokerTarget = schedulable.getTargetInvoker();
                    if (invokerTarget == null) {
                        LOG.warn("Activation with id {} has null target invoker and will not be processed.",
                                schedulable.getActivationId());
                        continue;
                    }
                    if (!invokersHealth.containsKey(invokerTarget) ||
                            invokersHealth.get(invokerTarget).getKey() != State.HEALTHY) {
                        ConcurrentLinkedDeque<ISchedulable> invokerQueue =
                                invokersHealthQueue.get(invokerTarget);
                        if (invokerQueue == null) {
                            invokerQueue = new ConcurrentLinkedDeque<>();
                            invokersHealthQueue.put(invokerTarget, invokerQueue);
                        }
                        invokerQueue.add(schedulable);
                        continue;
                    }
                }
                invocationQueue.add(consumable);
            }
            if (invocationQueue.size() > 0)
                super.newEvent(stream, invocationQueue);
        }
        healthyChecks(healthyChecks);
        removeOldEntries(timeLimitMs);
    }

    /**
     *
     *
     * @param delta
     */
    private void healthyChecks(long delta) {
        long now = Instant.now().toEpochMilli();
        for (final Map.Entry<String, Map.Entry<State, Long>> invokerStateEntry : invokersHealth.entrySet()) {
            if (now - invokerStateEntry.getValue().getValue() > delta) {
                invokerStateEntry.setValue(new AbstractMap.SimpleEntry<>(
                        State.UNHEALTHY, Instant.now().toEpochMilli()));
                LOG.trace("Marked {} invoker as {}.", invokerStateEntry.getKey(), State.UNHEALTHY);
            }
        }
    }

    /**
     * Remove entries older than {@link #timeLimitMs}.
     */
    private void removeOldEntries(long delta) {
        long now = Instant.now().toEpochMilli();
        final List<String> invokersTarget = new ArrayList<>();
        for (final Map.Entry<String, Map.Entry<State, Long>> invokerStateEntry : invokersHealth.entrySet()) {
            if (now - invokerStateEntry.getValue().getValue() > delta)
                invokersTarget.add(invokerStateEntry.getKey());
        }
        if (invokersTarget.size() > 0) {
            LOG.trace("Updating invokersHealth map - removed {} elements from invokers map (retention {} ms).",
                    invokersTarget.size(), delta);
            invokersHealth.keySet().removeIf(invokersTarget::contains);
            invokersHealthQueue.keySet().removeIf(invokersTarget::contains);
        }
    }

}