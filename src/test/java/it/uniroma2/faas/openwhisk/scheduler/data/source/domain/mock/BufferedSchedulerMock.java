package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.AdvancedScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.BufferedScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.IBufferizable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.Invoker;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Instance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.ActivationKafkaConsumerMock.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.CompletionKafkaConsumerMock.COMPLETION_STREAM;

public class BufferedSchedulerMock extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(BufferedScheduler.class.getCanonicalName());

    private final Object mutex = new Object();
    // <targetInvoker, Invoker>
    private final Map<String, Invoker> invokersMap = new HashMap<>(24);
    //
    private final Queue<IBufferizable> bufferizableQueue = new ArrayDeque<>();

    public BufferedSchedulerMock(Scheduler scheduler) {
        super(scheduler);
    }

    // TODO: should this method be synchronized?

    /**
     * Note: Apache OpenWhisk Controller component knows if invokers are overloaded, so upon
     * @param stream
     * @param data
     */
    @Override
    public void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data) {
        checkNotNull(stream, "Stream can not be null.");
        checkNotNull(data, "Data can not be null.");

        if (stream.equals(ACTIVATION_STREAM)) {
            final Collection<IBufferizable> bufferizables = data.stream()
                    .filter(IBufferizable.class::isInstance)
                    .map(IBufferizable.class::cast)
                    .collect(Collectors.toCollection(ArrayDeque::new));
            if (bufferizables.size() != data.size()) {
                LOG.warn("Non bufferizable objects discarded: {}.", data.size() - bufferizables.size());
            }

            final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
            synchronized (mutex) {
                for (final IBufferizable bufferizable : bufferizables) {
                    Invoker invoker = invokersMap.get(bufferizable.getTargetInvoker());
                    if (invoker == null) {
                        final long newInvokerUserMemory = bufferizable.getUserMemory() == null
                                ? 2048L
                                // OPTIMIZE: create utils for manage byte unit like java.util.concurrent.TimeUnit
                                : bufferizable.getUserMemory() / (1024 * 1024);
                        final Invoker newInvoker = new Invoker(bufferizable.getTargetInvoker(), newInvokerUserMemory);
                        invokersMap.put(bufferizable.getTargetInvoker(), newInvoker);
                        invoker = newInvoker;
                    }
                    if (invoker.tryAcquireMemoryAndConcurrency(bufferizable)) {
                        invocationQueue.add(bufferizable);
                        LOG.trace("Scheduled activation {} - remaining memory on {}: {}.",
                                bufferizable.getActivationId(), invoker.getInvokerName(), invoker.getMemory());
                    } else {
                        bufferizableQueue.add(bufferizable);
                        LOG.trace("System overloading (controller flag: {}), buffering activation {} for target {}.",
                                bufferizable.getOverload(), bufferizable.getActivationId(), bufferizable.getTargetInvoker());
                    }
                }
                if (bufferizableQueue.size() > 0) {
                    LOG.trace("System overloading, buffering {} activations.", bufferizables.size());
                }
            }
            super.newEvent(stream, invocationQueue);
        } else if (stream.equals(COMPLETION_STREAM)) {
            final Collection<? extends Completion> completions = data.stream()
                    .filter(Completion.class::isInstance)
                    .map(Completion.class::cast)
                    .collect(Collectors.toCollection(ArrayDeque::new));
            LOG.trace("Processing {} completion objects (over {} received).",
                    completions.size(), data.size());
            if (completions.size() > 0) {
                synchronized (mutex) {
                    for (final Completion completion : completions) {
                        final Invoker invoker = invokersMap.get(getInvokerTargetFrom(completion.getInstance()));
                        final String activationId = completion.getActivationId() == null
                                ? completion.getResponse().getActivationId()
                                : completion.getActivationId();
                        invoker.release(activationId);
                    }
                }
            }
        } else {
            LOG.trace("Pass through stream {}.", stream.toString());
            super.newEvent(stream, data);
        }
    }

    private void createNewSubject() {
        // TODO: create new subject for consuming Completion
    }

    private void removeOldSubjects() {
        // TODO: remove subject which not emit Completion for a while to free resources
    }

    public static @Nonnull String getInvokerTargetFrom(@Nonnull Instance instance) {
        checkNotNull(instance, "Instance can not be null.");
        return instance.getInstanceType().getName() + instance.getInstance();
    }

}