package it.uniroma2.faas.openwhisk.scheduler.scheduler;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IProducer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.ISchedulable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.Policy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;

/**
 * BaseScheduler provides scheduling functionality only for {@link Activation} objects.
 * Scheduling functionality are provided by selected {@link IPolicy}.
 * Valid {@link ISchedulable}s, after scheduling process, are dispatched by specified {@link IProducer}.
 */
public class BaseScheduler extends Scheduler {

    private final static Logger LOG = LogManager.getLogger(BaseScheduler.class.getCanonicalName());

    private final List<ISubject> subjects = new ArrayList<>();
    private final IPolicy policy;
    private final IProducer producer;

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

    public Policy getPolicy() {
        return policy.getPolicy();
    }

    @Override
    public <T> void newEvent(@Nonnull UUID stream, @Nonnull final Collection<T> data) {
        checkNotNull(stream, "Stream can not be null.");
        checkNotNull(data, "Data can not be null.");

        // TODO: in caso ci siano più consumer su threads diversi che invocano questo metodo,
        //      è necessario usare mutex
        // see@ https://stackoverflow.com/questions/3741765/ordering-threads-to-run-in-the-order-they-were-created-started
        // see@ https://stackoverflow.com/questions/12286628/which-thread-will-be-the-first-to-enter-the-critical-section

        if (!stream.equals(ACTIVATION_STREAM)) {
            LOG.warn("Unable to manage data type from stream {}.", stream.toString());
            return;
        }

        LOG.trace("Processing new event from stream {}.", stream);
        // IObserver and ISubject interface are decoupled from IConsumer, so <T> type could contains
        //   non schedulables objects
        final Collection<ISchedulable> schedulables = data.stream()
                .filter(ISchedulable.class::isInstance)
                .map(ISchedulable.class::cast)
                .collect(Collectors.toCollection(() -> new ArrayDeque<>(data.size())));
        final Collection<T> nonSchedulable = data.stream()
                .filter(e -> !schedulables.contains(e))
                .collect(Collectors.toUnmodifiableList());
        if (nonSchedulable.size() > 0) {
            LOG.warn("Non schedulables objects ({}) found in stream {}.", nonSchedulable.size(), stream.toString());
        }
        LOG.trace("Processing {} schedulables objects (over {} received).", schedulables.size(), data.size());
        send(policy.apply(schedulables));
    }

    private void send(@Nonnull final Queue<ISchedulable> schedulables) {
        checkNotNull(schedulables, "Schedulables to send can not be null.");

        ISchedulable schedulable = schedulables.poll();
        while (schedulable != null) {
            // if activation has not target invoker, abort its processing
            if (schedulable.getTargetInvoker() == null) {
                LOG.warn("Invalid target invoker (null) for activation with id {}.",
                        schedulable.getActivationId());
            } else {
                LOG.trace("Writing activation with id {} in {} topic.",
                        schedulable.getActivationId(), schedulable.getTargetInvoker());
                producer.produce(schedulable.getTargetInvoker(), schedulable);
            }
            schedulable = schedulables.poll();
        }
    }

}