package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IProducer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.*;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.IBufferizable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.Invoker;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerExecutors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer.COMPLETION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.HealthKafkaConsumer.HEALTH_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.Invoker.State.*;
import static java.util.stream.Collectors.*;

public class BufferedScheduler extends Scheduler {

    private final static Logger LOG = LogManager.getLogger(BufferedScheduler.class.getCanonicalName());

    public static final String TEMPLATE_COMPLETION_TOPIC = "completed%d";
    public static final int THREAD_COUNT = 2;
    public static final long HEALTH_CHECK_TIME_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long OFFLINE_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);
    public static final long RUNNING_ACTIVATION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);
    public static final int MAX_BUFFER_SIZE = 500;

    // time after which an invoker is marked as unhealthy
    private long healthCheckTimeLimitMs = HEALTH_CHECK_TIME_MS;
    // time after which an invoker is marked as offline
    private long offlineTimeLimitMs = OFFLINE_TIME_LIMIT_MS;
    // time after which running activations on invokers are removed
    //   (security measure to not block invoker in case completion message is missing)
    private long runningActivationTimeLimitMs = RUNNING_ACTIVATION_TIME_LIMIT_MS;
    // per invoker max queue size
    protected int maxBufferSize = MAX_BUFFER_SIZE;
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

    private final List<ISubject> subjects = new ArrayList<>();
    // subclass can use scheduler's policy
    private final IPolicy policy;
    private final IProducer producer;

    public BufferedScheduler(@Nonnull IPolicy policy, @Nonnull IProducer producer) {
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

    /**
     * Note: Apache OpenWhisk Controller component knows if invokers are overloaded, so upon
     * @param stream
     * @param data
     */
    @Override
    public void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data) {
        checkNotNull(stream, "Stream can not be null.");
        checkNotNull(data, "Data can not be null.");

        // check if some invoker turned unhealthy or offline
        healthCheck(healthCheckTimeLimitMs, offlineTimeLimitMs);

        if (stream.equals(ACTIVATION_STREAM)) {
            final Collection<IBufferizable> bufferizables = data.stream()
                    .filter(IBufferizable.class::isInstance)
                    .map(IBufferizable.class::cast)
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[ACT] - Processing {} bufferizables objects (over {} received).",
                    bufferizables.size(), data.size());

            if (bufferizables.size() > 0) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue;
                synchronized (mutex) {
                    // release activations too old, assuming there was an error sending completion
                    // check performed in this branch to reduce its frequency
                    //   (should not be performed in completion branch in case completion
                    //   Kafka consumers fail)
                    releaseActivationsOlderThan(runningActivationTimeLimitMs);

                    // insert all received elements in the buffer,
                    //   reorder the buffer using selected policy
                    buffering(bufferizables);
                    // remove from buffer all activations that can be processed on invokers
                    //   (so, invoker has sufficient resources to process the activations)
                    invocationQueue = new ArrayDeque<>(pollAndAcquireResourcesForAllFlattened(
                            new ArrayList<>(invokersMap.keySet())));
                }
                // send activations
                if (!invocationQueue.isEmpty()) send(invocationQueue);
            }
        } else if (stream.equals(COMPLETION_STREAM)) {
            final Collection<? extends Completion> completions = data.stream()
                    .filter(Completion.class::isInstance)
                    .map(Completion.class::cast)
                    .collect(Collectors.toCollection(ArrayDeque::new));
            LOG.trace("[CMP] - Processing {} completion objects (over {} received).",
                    completions.size(), data.size());
            if (completions.size() > 0) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
                synchronized (mutex) {
                    // <invokerTarget, completionCount>
                    // mapping between invoker and its processed completions
                    final Map<String, Integer> invokerCompletionCountMap = new HashMap<>(invokersMap.size());

                    // first, free resources for all received completions
                    for (final Completion completion : completions) {
                        final String invokerTarget = getInvokerTargetFrom(completion.getInstance());
                        final String activationId;
                        if (completion instanceof NonBlockingCompletion) {
                            activationId = ((NonBlockingCompletion) completion).getActivationId();
                        } else if (completion instanceof BlockingCompletion) {
                            activationId = ((BlockingCompletion) completion).getResponse().getActivationId();
                        } else if (completion instanceof FailureCompletion) {
                            activationId = ((FailureCompletion) completion).getResponse();
                        } else {
                            LOG.trace("Failing to process completion from invoker {}.", invokerTarget);
                            continue;
                        }
                        final Invoker invoker = invokersMap.get(invokerTarget);

                        // there is no reference to this invoker in the system
                        if (invoker == null) continue;
                        // release resources associated with this completion (even if invoker is not healthy)
                        invoker.release(activationId);

                        // if invoker target is healthy, insert max completionsCount of activations
                        //   that could be scheduled on it
                        Integer completionsCount = invokerCompletionCountMap.putIfAbsent(invokerTarget, 1);
                        if (completionsCount != null) invokerCompletionCountMap.put(invokerTarget, ++completionsCount);
                    }

                    // second, for all invokers that have produced at least one completion,
                    //   check if there is a buffered activation that can be scheduled on it
                    //   (so, if it has necessary resources)
                    if (!invokerCompletionCountMap.isEmpty())
                        invocationQueue.addAll(
                                pollAndAcquireAtMostResourcesForAllFlattened(invokerCompletionCountMap));
                }
                // send activations
                if (!invocationQueue.isEmpty()) send(invocationQueue);
            }
        } else if (stream.equals(HEALTH_STREAM)) {
            // TODO: manage case when an invoker get updated with more/less memory
            final Collection<? extends Health> heartbeats = data.stream()
                    .filter(Health.class::isInstance)
                    .map(Health.class::cast)
                    // get only unique hearth-beats messages
                    .distinct()
                    .collect(Collectors.toCollection(ArrayDeque::new));
            /*LOG.trace("[HLT] - Processing {} uniques hearth-beats objects (over {} received).",
                    heartbeats.size(), data.size());*/

            if (heartbeats.size() > 0) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
                synchronized (mutex) {
                    // contains id of invokers that have turned healthy
                    final List<String> invokersTurnedHealthy = new ArrayList<>();

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
                            invoker.setTimestamp(Instant.now().toEpochMilli());
                        // invoker has become healthy
                        } else {
                            // upon receiving hearth-beat from invoker, mark that invoker as healthy
                            invoker.updateState(HEALTHY, Instant.now().toEpochMilli());
                            LOG.trace("Invoker {} marked as {}.", invokerTarget, invoker.getState());

                            // add invoker to the list of invokers that have turned healthy
                            invokersTurnedHealthy.add(invokerTarget);
                        }
                    }
                    // remove all activations from buffer for which invokers
                    //   that have turned healthy can handle
                    if (!invokersTurnedHealthy.isEmpty())
                        invocationQueue.addAll(pollAndAcquireResourcesForAllFlattened(invokersTurnedHealthy));
                }
                // send activations
                if (!invocationQueue.isEmpty()) send(invocationQueue);
                checkIfCreateCompletionKafkaConsumers();
            }
        } else {
            LOG.trace("Unable to manage data from stream {}.", stream.toString());
        }
    }

    @Override
    public void shutdown() {
        synchronized (mutex) {
            LOG.trace("Closing {} completion kafka consumers.", invokerCompletionConsumerMap.size());
            invokerCompletionConsumerMap.values().forEach(CompletionKafkaConsumer::close);
        }
        executors.shutdown();
        LOG.trace("{} shutdown.", this.getClass().getSimpleName());
    }

    private void buffering(@Nonnull Collection<IBufferizable> bufferizables) {
        checkNotNull(bufferizables, "Input collection can not be null.");
        final Map<String, Collection<IBufferizable>> invokerCollectionMap = bufferizables.stream()
                .collect(groupingBy(IBufferizable::getTargetInvoker, toCollection(ArrayList::new)));
        buffering(invokerCollectionMap);
    }

    private void buffering(@Nonnull Map<String, Collection<IBufferizable>> invokerBufferizableMap) {
        invokerBufferizableMap.forEach(this::buffering);
    }

    private void buffering(@Nonnull String invoker, @Nonnull Collection<IBufferizable> bufferizables) {
        checkNotNull(invoker, "Invoker target can not be null.");
        checkNotNull(bufferizables, "Input collection can not be null.");
        if (bufferizables.isEmpty()) return;
        synchronized (mutex) {
            Queue<IBufferizable> buffer = invokerBufferMap.get(invoker);
            if (buffer == null) {
                buffer = new ArrayDeque<>();
                invokerBufferMap.put(invoker, buffer);
            }

            // remove old activations if buffer exceed its max size
            final int total = buffer.size() + bufferizables.size();
            if (total > maxBufferSize) {
                // creating new array dequeue to use #removeLast method
                buffer = new ArrayDeque<>(buffer);
                final int toRemove = total - maxBufferSize;
                for (int i = 0; i < toRemove; ++i) ((ArrayDeque<IBufferizable>) buffer).removeLast();
                LOG.trace("Reached buffer limit ({}) for invoker {} - discarding last {} activations.",
                        maxBufferSize, invoker, toRemove);
            }
            // add all new activations
            buffer.addAll(bufferizables);

            invokerBufferMap.put(invoker, (Queue<IBufferizable>) policy.apply(buffer));
        }
    }

    private @Nonnull Queue<IBufferizable> pollAndAcquireResourcesForAllFlattened(@Nonnull List<String> invokers) {
        final Map<String, Queue<IBufferizable>> invokerQueueMap = pollAndAcquireResourcesForAll(invokers);
        return invokerQueueMap.values().stream()
                .flatMap(Queue::stream)
                .collect(Collectors.toCollection(ArrayDeque::new));
    }

    private @Nonnull Map<String, Queue<IBufferizable>> pollAndAcquireResourcesForAll(@Nonnull List<String> invokers) {
        checkNotNull(invokers, "Invokers list can not be null.");
        final Map<String, Integer> invokerCountMap = new HashMap<>(invokers.size());
        for (final String invoker : invokers) {
            invokerCountMap.put(invoker, Integer.MAX_VALUE);
        }
        return pollAndAcquireAtMostResourcesForAll(invokerCountMap).entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private @Nonnull Queue<IBufferizable> pollAndAcquireAtMostResourcesForAllFlattened(@Nonnull Map<String, Integer> invokers) {
        final Map<String, Queue<IBufferizable>> invokerQueueMap = pollAndAcquireAtMostResourcesForAll(invokers);
        return invokerQueueMap.values().stream()
                .filter(Objects::nonNull)
                .filter(Predicate.not(Collection::isEmpty))
                .flatMap(Queue::stream)
                .collect(Collectors.toCollection(ArrayDeque::new));
    }

    private @Nonnull Map<String, Queue<IBufferizable>> pollAndAcquireAtMostResourcesForAll(@Nonnull Map<String, Integer> invokers) {
        checkNotNull(invokers, "Invokers list can not be null.");
        final Map<String, Queue<IBufferizable>> invokerBufferizableMap = new HashMap<>(invokers.size());
        synchronized (mutex) {
            // second, for all invokers specified check if there is a buffered activation
            //   that can be scheduled on it (so, if it has necessary resources)
            for (final Map.Entry<String, Integer> entry : invokers.entrySet()) {
                final String invokerTarget = entry.getKey();
                final Invoker invoker = invokersMap.get(invokerTarget);

                // if invoker target is not healthy, so no new activations can be scheduled on it
                if (invoker == null || !invoker.isHealthy()) continue;

                // elements inside buffer are already sorted, using selected policy, in the order
                //   they should be submitted
                final Queue<IBufferizable> buffer = invokerBufferMap.get(invokerTarget);
                // if there are no buffered activations for current invoker, check next
                if (buffer == null || buffer.isEmpty()) {
                    // null queue to indicate that buffer is empty
                    invokerBufferizableMap.put(invokerTarget, null);
                    continue;
                }

                // for all completions processed by current invoker, submit all buffered activations
                //   for which invoker has sufficient resources
                Integer count = entry.getValue();
                // double check
                if (count == null) {
                    LOG.warn("Map contains invokerTarget key but no value for count.");
                    continue;
                }
                // invocation queue
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
                // using iterator to remove activation from source buffer if condition is met
                // all invoker health test actions are inserted in the invocation queue and removed from buffer
                //   without consuming resources
                final Iterator<IBufferizable> bufferIterator = buffer.iterator();
                while (bufferIterator.hasNext()) {
                    final IBufferizable activation = bufferIterator.next();

                    // invoker health test action must always be processed, if in buffer, otherwise
                    //   Apache OpenWhisk Controller component will mark invoker unavailable
                    if (isInvokerHealthTestAction(activation.getAction())) {
                        // add to invocation queue
                        // the invocation order is not much important as all activation inserted
                        //   should be sent by producer
                        invocationQueue.add(activation);
                        // remove from original buffer but NOT acquire resources
                        bufferIterator.remove();
                        continue;
                    }

                    // select at most count activations from buffer
                    if (count > 0) {
                        // checks if at least one of buffered activations can be submitted
                        // note that it is not sufficient break iteration if can not be acquired
                        //   memory and concurrency on current activation because it is possible that
                        //   there is another activation in the buffered queue with requirements
                        //   to be scheduled on current invoker
                        if (invoker.tryAcquireMemoryAndConcurrency(activation)) {
                            invocationQueue.add(activation);
                            bufferIterator.remove();
                            --count;
                        }
                    }
                }
                // if there is at least one activation which fulfill requirements, add to map
                // adding empty queue to indicate that, on current invoker,
                //   there are not available resources
                invokerBufferizableMap.put(invokerTarget, invocationQueue);
            }
        }
        final Map<String, String> invokerQueueTrace = invokerBufferizableMap.entrySet().stream()
                .map(entry -> {
                    if (entry.getValue() == null) {
                        return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), "empty buffer");
                    } else if (entry.getValue().isEmpty()) {
                        return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), "not enough resources");
                    } else {
                        return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(),
                                String.format("%s activations", entry.getValue().size()));
                    }
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        final Map<String, String> invokerMemoryTrace = invokersMap.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(
                        entry.getKey(), entry.getValue().getMemory() + " MiB remaining"))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        LOG.trace("Scheduling - {}.", invokerQueueTrace);
        LOG.trace("Memory - {}.", invokerMemoryTrace);
        LOG.trace("Buffer - {}.", invokerBufferMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size())));
        return invokerBufferizableMap;
    }

    private void send(@Nonnull final Queue<? extends ISchedulable> schedulables) {
        checkNotNull(schedulables, "Schedulables to send can not be null.");
        final long schedulingTermination = Instant.now().toEpochMilli();
        schedulables.forEach(schedulable -> {
            // if activation has not target invoker, abort its processing
            if (schedulable.getTargetInvoker() == null) {
                LOG.warn("Invalid target invoker (null) for activation with id {}.",
                        schedulable.getActivationId());
            } else {
                LOG.trace("Writing activation with id {} (priority {}) in {} topic.",
                        schedulable.getActivationId(), schedulable.getPriority(), schedulable.getTargetInvoker());
                producer.produce(schedulable.getTargetInvoker(),
                        Collections.singleton(schedulable.with(schedulingTermination)));
            }
        });
    }

    private void healthCheck(long healthCheck, long offlineCheck) {
        checkArgument(healthCheck < offlineCheck,
                "Offline check time must be > of health check time.");
        long now = Instant.now().toEpochMilli();
        synchronized (mutex) {
            for (final Invoker invoker : invokersMap.values()) {
                // if invoker has not sent hearth-beat in delta, mark it as offline
                if (now - invoker.getTimestamp() > offlineCheck) {
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
                    // continue because if invoker is offline is also unhealthy
                    continue;
                }

                // if invoker has not sent hearth-beat in delta, mark it as unhealthy
                if (now - invoker.getTimestamp() > healthCheck) {
                    // timestamp of the last update will not be updated
                    invoker.updateState(UNHEALTHY);
                    LOG.trace("Invoker {} marked as {}.", invoker.getInvokerName(), invoker.getState());
                }
            }
        }
    }

    /**
     * For each invoker, release resources associated with activations older than delta.
     *
     * @param delta time after which remove activations.
     */
    private void releaseActivationsOlderThan(long delta) {
        checkArgument(delta >= 0, "Delta time must be >= 0.");
        final Map<String, Long> invokerOldActivationsMapTrace = new HashMap<>();
        synchronized (mutex) {
            for (final Invoker invoker : invokersMap.values()) {
                final long activationsBefore = invoker.getActivationsCount();
                invoker.releaseAllOldThan(delta);
                final long activationsAfter = invoker.getActivationsCount();
                if (activationsAfter < activationsBefore) {
                    invokerOldActivationsMapTrace.put(invoker.getInvokerName(),
                            activationsBefore - activationsAfter);
                }
            }
        }
        if (!invokerOldActivationsMapTrace.isEmpty())
            LOG.trace("Removed old activations from invokers (time delta - {} ms) - {}.",
                    delta, invokerOldActivationsMapTrace);
    }

    private void checkIfCreateCompletionKafkaConsumers() {
        synchronized (mutex) {
            for (final String invokerTarget : invokersMap.keySet()) {
                CompletionKafkaConsumer completionKafkaConsumer =
                        invokerCompletionConsumerMap.get(invokerTarget);
                // if there are completion Kafka consumers marked as null, they must be created
                if (completionKafkaConsumer == null) {
                    final int invokerInstance = getInstanceFromInvokerTarget(invokerTarget);
                    // OPTIMIZE: should Kafka consumer creation be placed
                    //   outside synchronized block ?
                    invokerCompletionConsumerMap.put(invokerTarget,
                            createCompletionConsumerFrom(invokerInstance));
                }
            }
        }
    }

    private @Nonnull CompletionKafkaConsumer createCompletionConsumerFrom(int instance) {
        final String topic = String.format(TEMPLATE_COMPLETION_TOPIC, instance);
        LOG.trace("Creating new completion Kafka consumer for topic: {}.", topic);
        final CompletionKafkaConsumer completionKafkaConsumer = new CompletionKafkaConsumer(
                List.of(topic), kafkaConsumerProperties, 100
        );
        register(List.of(completionKafkaConsumer));
        completionKafkaConsumer.register(List.of(this));
        Objects.requireNonNull(executors.networkIO()).submit(completionKafkaConsumer);
        return completionKafkaConsumer;
    }

    public static @Nonnull String getInvokerTargetFrom(@Nonnull Instance instance) {
        checkNotNull(instance, "Instance can not be null.");
        return instance.getInstanceType().getName() + instance.getInstance();
    }

    public static int getInstanceFromInvokerTarget(@Nonnull String invokerTarget) {
        checkNotNull(invokerTarget, "Invoker target can not be null.");
        return Integer.parseInt(invokerTarget.replace("invoker", ""));
    }

    public static boolean isInvokerHealthTestAction(@Nullable Action action) {
        if (action == null) return false;
        return action.getPath().equals("whisk.system") &&
                action.getName().matches("^invokerHealthTestAction[0-9]+$");
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

    public long getHealthCheckTimeLimitMs() {
        return healthCheckTimeLimitMs;
    }

    public void setHealthCheckTimeLimitMs(long healthCheckTimeLimitMs) {
        this.healthCheckTimeLimitMs = healthCheckTimeLimitMs;
    }

    public long getOfflineTimeLimitMs() {
        return offlineTimeLimitMs;
    }

    public void setOfflineTimeLimitMs(long offlineTimeLimitMs) {
        this.offlineTimeLimitMs = offlineTimeLimitMs;
    }

    public long getRunningActivationTimeLimitMs() {
        return runningActivationTimeLimitMs;
    }

    public void setRunningActivationTimeLimitMs(long runningActivationTimeLimitMs) {
        this.runningActivationTimeLimitMs = runningActivationTimeLimitMs;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

}