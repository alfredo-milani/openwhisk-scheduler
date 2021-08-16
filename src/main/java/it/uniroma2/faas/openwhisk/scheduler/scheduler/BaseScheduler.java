package it.uniroma2.faas.openwhisk.scheduler.scheduler;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IProducer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerExecutors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;
import static java.util.stream.Collectors.toCollection;

/**
 * BaseScheduler provides scheduling functionality only for {@link Activation} objects.
 * Scheduling functionality are provided by selected {@link IPolicy}.
 * Valid {@link ISchedulable}s, after scheduling process, are dispatched by specified {@link IProducer}.
 */
public class BaseScheduler extends Scheduler {

    private final static Logger LOG = LogManager.getLogger(BaseScheduler.class.getCanonicalName());

    public static final int THREAD_COUNT_KAFKA_PRODUCERS = 5;

    private final List<ISubject> subjects = new ArrayList<>();
    // subclass can use scheduler's policy
    private final IPolicy policy;
    private final IProducer producer;
    // over-allocate invoker's resource to let activations queue up on invoker
    private final SchedulerExecutors schedulerExecutors = new SchedulerExecutors(
            THREAD_COUNT_KAFKA_PRODUCERS,
            0
    );

    public BaseScheduler(@Nonnull IPolicy policy, @Nonnull IProducer producer) {
        checkNotNull(policy, "Policy can not be null.");
        checkNotNull(producer, "Producer can not be null.");

        this.policy = policy;
        this.producer = producer;
    }

    @Override
    public void register(@Nonnull List<ISubject> subjects) {
        checkNotNull(subjects, "Subjects can not be null.");
        this.subjects.addAll(subjects);
    }

    @Override
    public void unregister(@Nonnull List<ISubject> subjects) {
        checkNotNull(subjects, "Subjects can not be null.");
        this.subjects.removeAll(subjects);
    }

    @Override
    public void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data) {
        // see@ https://stackoverflow.com/questions/3741765/ordering-threads-to-run-in-the-order-they-were-created-started
        // see@ https://stackoverflow.com/questions/12286628/which-thread-will-be-the-first-to-enter-the-critical-section

        if (!stream.equals(ACTIVATION_STREAM)) {
            LOG.trace("Unable to manage data from stream {}.", stream.toString());
            return;
        }

        final Collection<ISchedulable> newActivations = data.stream()
                .filter(ISchedulable.class::isInstance)
                .map(ISchedulable.class::cast)
                .collect(toCollection(ArrayDeque::new));
        LOG.trace("[ACT] Processing {} activations objects (over {} received).",
                newActivations.size(), data.size());

        if (!newActivations.isEmpty())
            Objects.requireNonNull(schedulerExecutors.networkIO())
                    .execute(() -> send(producer, policy.apply(newActivations)));
    }

    private void send(@Nonnull final IProducer producer,
                      @Nonnull final Queue<? extends ISchedulable> schedulables) {
        final long schedulingTermination = Instant.now().toEpochMilli();
        for (final ISchedulable schedulable : schedulables) {
            // if activation has not target invoker, abort its processing
            if (schedulable.getTargetInvoker() == null) {
                LOG.warn("Invalid target invoker (null) for activation with id {}.",
                        schedulable.getActivationId());
            } else {
                LOG.trace("Writing activation with id {} in {} topic.",
                        schedulable.getActivationId(), schedulable.getTargetInvoker());
                producer.produce(schedulable.getTargetInvoker(),
                        (IConsumable) schedulable.with(schedulingTermination));
            }
        }
    }

    @Override
    public void shutdown() {
        LOG.info("{} shutdown.", this.getClass().getSimpleName());
    }

}