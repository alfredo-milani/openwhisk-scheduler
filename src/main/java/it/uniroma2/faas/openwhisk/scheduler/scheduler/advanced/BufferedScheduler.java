package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.IBufferizable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.Invoker;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Instance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer.COMPLETION_STREAM;

public class BufferedScheduler extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(BufferedScheduler.class.getCanonicalName());

    public static final int DEFAULT_THREAD_COUNT = 5;

    private final ExecutorService executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
    private final Object mutex = new Object();
    // <targetInvoker, Invoker>
    // invoker's kafka topic name used as unique id for invoker
    // Note: must be externally synchronized to safely access Invoker entities
    private final Map<String, Invoker> invokersMap = new HashMap<>(24);
    //
    private final Queue<IBufferizable> bufferizablesQueue = new ArrayDeque<>();
    //
    private final Map<String, State> invokersHealth = new ConcurrentHashMap<>(24);
    //
    private final Map<String, Queue<? extends ISchedulable>> invokersHealthQueue = new HashMap<>(24);
    // represent invoker's state
    private enum State {
        HEALTHY,
        UNHEALTHY,
        OFFLINE
    }

    public BufferedScheduler(Scheduler scheduler) {
        super(scheduler);
    }

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
                        // TODO: nel caso si utilizzi il topic "health" per la generazione degli invokers
                        //   si deve creare una coda contenente le activation che non hanno ancora un invoker oppure
                        //   va bene metterle nella coda bufferizedQueue ?
                        final long newInvokerUserMemory = bufferizable.getUserMemory() == null
                                ? 2048L // TODO:
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
                        bufferizablesQueue.add(bufferizable);
                        LOG.trace("System overloading (controller flag: {}), buffering activation {} for target {}.",
                                bufferizable.getOverload(), bufferizable.getActivationId(), bufferizable.getTargetInvoker());
                    }
                }
                if (bufferizablesQueue.size() > 0) {
                    LOG.trace("System overloading, buffering {} activations.", bufferizables.size());
                }
                // TODO: vedere se metterlo fuori da synchronized
                super.newEvent(stream, invocationQueue);
            }
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
                // TODO: vedere se inviare elementi della coda
            }
        } else {
            LOG.trace("Pass through stream {}.", stream.toString());
            super.newEvent(stream, data);
        }

        // TODO: UTILIZZARE HEALTH TOPIC PER REGISTRARE GLI INVOKERS E REGISTRARE I CONSUMERS DI COMPLETION-N ?
        // TODO: inserire shutdown() metodo per fare cleanup di consumers creati da scheduler ?
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