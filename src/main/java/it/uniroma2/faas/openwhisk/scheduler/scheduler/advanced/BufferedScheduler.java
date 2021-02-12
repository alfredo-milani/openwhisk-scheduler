package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.IBufferizable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.Invoker;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Health;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Instance;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerExecutors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer.COMPLETION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.HealthKafkaConsumer.HEALTH_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.Invoker.State.*;

public class BufferedScheduler extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(BufferedScheduler.class.getCanonicalName());

    public static final String TEMPLATE_COMPLETION_TOPIC = "completed%d";
    public static final int THREAD_COUNT = 2;
    public static final long HEALTH_CHECK_TIME_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long OFFLINE_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);

    private long healthCheckTimeLimitMs = HEALTH_CHECK_TIME_MS;
    private long offlineTimeLimitMs = OFFLINE_TIME_LIMIT_MS;
    private final SchedulerExecutors executors = new SchedulerExecutors(THREAD_COUNT, 0);
    private final Object mutex = new Object();
    // <targetInvoker, Invoker>
    // invoker's kafka topic name used as unique id for invoker
    // Note: must be externally synchronized to safely access Invoker entities
    private final Map<String, Invoker> invokersMap = new HashMap<>(24);
    // <targetInvoker, Buffer>
    // contains a mapping between an invoker id and its buffered activations
    private final Map<String, Queue<IBufferizable>> invokerBufferMap = new HashMap<>(24);
    // <targetInvoker, CompletionKafkaConsumer>
    // contains all completion Kafka consumer associated with healthy invokers
    private final Map<String, CompletionKafkaConsumer> invokerCompletionConsumerMap = new HashMap<>(24);
    // default kafka properties for all completion Kafka consumers
    private final Properties kafkaConsumerProperties = new Properties() {{
        put(ConsumerConfig.GROUP_ID_CONFIG, "ow-scheduler-consumer");
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1_000);
        put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15_000);
        put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 0);
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }};

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
                    final String invokerTarget = bufferizable.getTargetInvoker();
                    final Invoker invoker = invokersMap.get(bufferizable.getTargetInvoker());

                    // if there is not yet target invoker in the system or target invoker
                    //   is not healthy, buffer activation
                    if (invoker == null || !invoker.isHealthy()) {
                        invokerBufferMap.putIfAbsent(invokerTarget, new ArrayDeque<>());
                        invokerBufferMap.get(invokerTarget).add(bufferizable);
                        LOG.trace("Invoker {} not yet registered or not healthy. Buffering activation with id {}.",
                                invokerTarget, bufferizable.getActivationId());
                        continue;
                    }

                    // try acquire resource on target invoker
                    if (invoker.tryAcquireMemoryAndConcurrency(bufferizable)) {
                        invocationQueue.add(bufferizable);
                        LOG.trace("Scheduled activation {} - remaining memory on {}: {}.",
                                bufferizable.getActivationId(), invoker.getInvokerName(), invoker.getMemory());
                    // target invoker overloaded, so buffer activation
                    } else {
                        invokerBufferMap.putIfAbsent(invokerTarget, new ArrayDeque<>());
                        invokerBufferMap.get(invokerTarget).add(bufferizable);
                        LOG.trace("Detected system overload (controller flag: {}), buffering activation with id {} for target {}.",
                                bufferizable.getOverload(), bufferizable.getActivationId(), bufferizable.getTargetInvoker());
                    }
                }
            }
            if (invocationQueue.size() > 0)
                super.newEvent(stream, invocationQueue);
        } else if (stream.equals(COMPLETION_STREAM)) {
            final Collection<? extends Completion> completions = data.stream()
                    .filter(Completion.class::isInstance)
                    .map(Completion.class::cast)
                    .collect(Collectors.toCollection(ArrayDeque::new));
            LOG.trace("Processing {} completion objects (over {} received).",
                    completions.size(), data.size());
            if (completions.size() > 0) {
                // contains activations to be sent
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
                // <invokerTarget, completionCount>
                // mapping between invoker and its processed completions
                final Map<String, Long> invokerCompletionCountMap = new HashMap<>();
                synchronized (mutex) {
                    // first, free resources for all received completions
                    for (final Completion completion : completions) {
                        final String invokerTarget = getInvokerTargetFrom(completion.getInstance());
                        final String activationId = completion.getActivationId() == null
                                // if the request was blocking with result requested
                                ? completion.getResponse().getActivationId()
                                // if the request was non blocking
                                : completion.getActivationId();
                        final Invoker invoker = invokersMap.get(invokerTarget);

                        // there is no reference to this invoker in the system
                        if (invoker == null) continue;
                        // release resources associated with this completion (even if invoker is not healthy)
                        invoker.release(activationId);
                        // if invoker target is not healthy, so no new activations can be scheduled on it
                        if (!invoker.isHealthy()) continue;

                        // if invoker target is healthy, insert max count of activations
                        //   that could be scheduled on it
                        Long count = invokerCompletionCountMap.putIfAbsent(invokerTarget, 1L);
                        if (count != null) invokerCompletionCountMap.put(invokerTarget, ++count);
                    }

                    // second, for all invokers that have produced at least one completion,
                    //   check if there is a buffered activation that can be scheduled on it
                    //   (so, if it has necessary resources)
                    for (final Map.Entry<String, Long> entry : invokerCompletionCountMap.entrySet()) {
                        final String invokerTarget = entry.getKey();
                        final Invoker invoker = invokersMap.get(invokerTarget);

                        // double check on invoker
                        if (invoker == null || !invoker.isHealthy()) continue;

                        // OPTIMIZE: could be considered a queue containing activations ordered
                        //   as dictated by current scheduler's policy
                        final Queue<IBufferizable> buffer = invokerBufferMap.get(invokerTarget);
                        // if there are no buffered activations for current invoker, check next
                        if (buffer == null || buffer.size() == 0) continue;

                        // create new queue objects which contains the same elements from buffered queue
                        //   but in the order specified by current policy
                        final Queue<IBufferizable> bufferFromPolicy = applyPolicy(buffer);

                        // for all completions processed by current invoker, submit all buffered activations
                        //   for which invoker has sufficient resources
                        Long completionsCount = entry.getValue();
                        // double check
                        if (completionsCount == null) {
                            LOG.warn("Map contains invokerTarget key but no value for completionsCount.");
                            continue;
                        }
                        for (final IBufferizable activation : bufferFromPolicy) {
                            // checks if at least one of buffered activations can be submitted
                            // note that it is not sufficient break iteration if can not be acquired
                            //   memory and concurrency on current activation because it is possible that
                            //   there is another activation in the buffered queue with requirements
                            //   to be scheduled on current invoker
                            if (completionsCount <= 0 || !invoker.tryAcquireMemoryAndConcurrency(activation))
                                continue;
                            invocationQueue.add(activation);
                            buffer.remove(activation);
                            --completionsCount;
                        }
                    }
                }
                if (invocationQueue.size() > 0)
                    super.newEvent(ACTIVATION_STREAM, invocationQueue);
            }
        } else if (stream.equals(HEALTH_STREAM)) {
            // TODO: manage case when an invoker get updated with more/less memory
            final Collection<? extends Health> heartbeats = data.stream()
                    .filter(Health.class::isInstance)
                    .map(Health.class::cast)
                    // get only unique hearthbeat message
                    .distinct()
                    .collect(Collectors.toCollection(ArrayDeque::new));
            LOG.trace("Processing {} uniques hearthbeats objects (over {} received).",
                    heartbeats.size(), data.size());

            if (heartbeats.size() > 0) {
                synchronized (mutex) {
                    for (final Health health : heartbeats) {
                        final String invokerTarget = getInvokerTargetFrom(health.getInstance());
                        Invoker invoker = invokersMap.get(invokerTarget);

                        // invoker has not yet registered in the system
                        if (invoker == null) {
                            final Invoker newInvoker = new Invoker(invokerTarget, getUserMemoryFrom(health.getInstance()));
                            // register new invoker
                            invokersMap.put(invokerTarget, newInvoker);
                            invoker = newInvoker;
                            // create new completion Kafka consumer
                            // assign its value to null to create it later (in health checking phase)
                            invokerCompletionConsumerMap.putIfAbsent(invokerTarget, null);
                            LOG.trace("New invoker registered in the system: {}.", invokerTarget);
                        }

                        // if invoker was already healthy, update its timestamp
                        if (invoker.isHealthy()) {
                            invoker.setUpdate(Instant.now().toEpochMilli());
                        // invoker has become healthy
                        } else {
                            // upon receiving hearth-beat from invoker, mark that invoker as healthy
                            invoker.updateState(HEALTHY, Instant.now().toEpochMilli());
                            LOG.trace("Invoker {} marked as {}.", invokerTarget, invoker.getState());

                            final Queue<IBufferizable> buffer = invokerBufferMap.get(invokerTarget);
                            // if there are buffered activations, send them all to activation stream
                            //   and remove them from buffering queue
                            if (buffer != null && buffer.size() > 0) {
                                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>(buffer);
                                buffer.clear();
                                newEvent(ACTIVATION_STREAM, invocationQueue);
                            }
                        }
                    }
                }
            }
        } else {
            LOG.trace("Pass through stream {}.", stream.toString());
            super.newEvent(stream, data);
        }
        healthCheck(healthCheckTimeLimitMs, offlineTimeLimitMs);
    }

    @Override
    public void shutdown() {
        synchronized (mutex) {
            LOG.trace("Closing {} completion kafka consumers.", invokerCompletionConsumerMap.size());
            invokerCompletionConsumerMap.values().forEach(CompletionKafkaConsumer::close);
        }
        executors.shutdown();
        super.shutdown();
    }

    /**
     * Create new {@link IBufferizable} queue with the same elements from input queue but in the order
     * specified by selected policy.
     *
     * @param queue input queue.
     * @return new queue with elements in custom order.
     */
    private @Nonnull Queue<IBufferizable> applyPolicy(@Nonnull Queue<IBufferizable> queue) {
        checkNotNull(queue, "Queue can not be null.");
        return (Queue<IBufferizable>) getPolicy().apply(queue);
    }

    private void healthCheck(long healthCheck, long offlineCheck) {
        long now = Instant.now().toEpochMilli();
        synchronized (mutex) {
            for (final Invoker invoker : invokersMap.values()) {
                // if invoker has not sent hearth-beat in delta, mark it as unhealthy
                if (now - invoker.getUpdate() > healthCheck) {
                    // timestamp of the last update will not be updated
                    invoker.updateState(UNHEALTHY);
                    LOG.trace("Invoker {} marked as {}.", invoker.getInvokerName(), invoker.getState());
                }

                // if invoker has not sent hearth-beat in delta, mark it as unhealthy
                if (now - invoker.getUpdate() > offlineCheck) {
                    // timestamp of the last update will not be updated
                    invoker.updateState(OFFLINE);
                    invoker.removeAllContainers();
                    LOG.trace("Invoker {} marked as {}.", invoker.getInvokerName(), invoker.getState());
                    // when invoker is marked as offline, release resources associated with it
                    invokerBufferMap.remove(invoker.getInvokerName());
                    final CompletionKafkaConsumer completionKafkaConsumer =
                            invokerCompletionConsumerMap.remove(invoker.getInvokerName());
                    if (completionKafkaConsumer != null) {
                        // OPTIMIZE: should Kafka consumer shutdown be placed
                        //   outside synchronized block ?
                        completionKafkaConsumer.close();
                    }
                }

                CompletionKafkaConsumer completionKafkaConsumer =
                        invokerCompletionConsumerMap.get(invoker.getInvokerName());
                // if there are completion Kafka consumers marked as null, they must be created
                if (completionKafkaConsumer == null) {
                    final String topic = String.format(TEMPLATE_COMPLETION_TOPIC,
                            getInstanceFromInvokerTarget(invoker.getInvokerName()));
                    LOG.trace("Creating new completion Kafka consumer for topic: {}.", topic);
                    // OPTIMIZE: should Kafka consumer creation be placed
                    //   outside synchronized block ?
                    completionKafkaConsumer =  new CompletionKafkaConsumer(
                            List.of(topic), kafkaConsumerProperties, 100
                    );
                    invokerCompletionConsumerMap.put(invoker.getInvokerName(), completionKafkaConsumer);
                    register(List.of(completionKafkaConsumer));
                    completionKafkaConsumer.register(List.of(this));
                    Objects.requireNonNull(executors.networkIO()).submit(completionKafkaConsumer);
                }
            }
        }
    }

    private void createNewCompletionConsumerFor(int instance) {
        final String topic = String.format(TEMPLATE_COMPLETION_TOPIC, instance);
        LOG.trace("Creating new completion Kafka consumer for topic: {}.", topic);
        final CompletionKafkaConsumer completionKafkaConsumer = new CompletionKafkaConsumer(
                List.of(topic), kafkaConsumerProperties, 100
        );
        register(List.of(completionKafkaConsumer));
        completionKafkaConsumer.register(List.of(this));
        Objects.requireNonNull(executors.networkIO()).submit(completionKafkaConsumer);
    }

    public static @Nonnull String getInvokerTargetFrom(@Nonnull Instance instance) {
        checkNotNull(instance, "Instance can not be null.");
        return instance.getInstanceType().getName() + instance.getInstance();
    }

    public static long getInstanceFromInvokerTarget(@Nonnull String invokerTarget) {
        checkNotNull(invokerTarget, "Invoker target can not be null.");
        return Long.parseLong(invokerTarget.replace("invoker", ""));
    }

    /**
     * Return user-memory in MiB.
     *
     * @param instance
     * @return
     */
    public static long getUserMemoryFrom(@Nonnull Instance instance) {
        checkNotNull(instance, "Instance can not be null.");
        return Long.parseLong(instance.getUserMemory().split(" ")[0]) / (1024L * 1024L);
    }

    public void setKafkaBootstrapServers(@Nonnull String kafkaBootstrapServers) {
        this.kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    }

}