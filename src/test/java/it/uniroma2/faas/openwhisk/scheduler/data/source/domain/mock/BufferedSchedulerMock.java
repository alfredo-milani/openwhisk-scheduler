package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IProducer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.BufferedScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.*;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.IBufferizable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.Invoker;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerExecutors;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerPeriodicExecutors;
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
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.ActivationKafkaConsumerMock.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.CompletionKafkaConsumerMock.COMPLETION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.HealthKafkaConsumerMock.HEALTH_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.Invoker.State.*;
import static java.util.stream.Collectors.*;

public class BufferedSchedulerMock extends Scheduler {

    private final static Logger LOG = LogManager.getLogger(BufferedScheduler.class.getCanonicalName());

    public static final String TEMPLATE_COMPLETION_TOPIC = "completed%s";
    public static final int THREAD_COUNT = 3;
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
    private final SchedulerExecutors schedulerExecutors = new SchedulerExecutors(THREAD_COUNT, 0);
    private final SchedulerPeriodicExecutors schedulerPeriodicExecutors = new SchedulerPeriodicExecutors(0, THREAD_COUNT);
    private final Object mutex = new Object();
    // <targetInvoker, Invoker>
    // invoker's kafka topic name used as unique id for invoker
    // Note: must be externally synchronized to safely access Invoker entities
    private final Map<String, Invoker> invokersMap = new HashMap<>(24);
    // <targetInvoker, Buffer>
    // contains a mapping between an invoker id and its buffered activations
    private final Map<String, Queue<IBufferizable>> invokerBufferMap = new HashMap<>(24);
    // <controller, CompletionKafkaConsumer>
    // contains all completion Kafka consumer associated with controllers
    private final Map<RootControllerIndex, CompletionKafkaConsumerMock> controllerCompletionConsumerMap =
            new HashMap<>(12);
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

    public BufferedSchedulerMock(@Nonnull IPolicy policy, @Nonnull IProducer producer) {
        checkNotNull(policy, "Policy can not be null.");
        checkNotNull(producer, "Producer can not be null.");

        this.policy = policy;
        this.producer = producer;

        // scheduler periodic activities
        schedulePeriodicActivities();
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
     *
     *
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
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[ACT] - Processing {} bufferizables objects (over {} received).",
                    bufferizables.size(), data.size());

            if (bufferizables.size() > 0) {
                // filter unique controller instance from activation record and check if there is yet
                //   a completion kafka consumer, otherwise create it
                // for now, created completion kafka consumers run indefinitely, without a mechanism to check
                //   if there are still producer which publish on that topics, but it's ok
                final Set<RootControllerIndex> controllers = bufferizables.stream()
                        .map(IBufferizable::getRootControllerIndex)
                        .filter(Objects::nonNull)
                        .collect(toSet());
                // OPTIMIZE: for now there is no reason to get a lock to access controllerCompletionConsumerMap
                //   because there is only one thread accessing it
                /*synchronized (mutex) {
                    // create new consumer for controller instance if it is not yet present
                    controllers.removeAll(controllerCompletionConsumerMap.keySet());
                    controllers.forEach(c -> controllerCompletionConsumerMap.put(
                            c, createCompletionConsumerFrom(c.getAsString())
                    ));
                }*/
                // create new consumer for controller instance if it is not yet present
                controllers.removeAll(controllerCompletionConsumerMap.keySet());
                controllers.forEach(c -> controllerCompletionConsumerMap.put(
                        c, createCompletionConsumerFrom(c.getAsString())
                ));

                // invocation queue
                // add all invoker test action activations without acquiring any resource
                // invoker health test action must always be processed, otherwise
                //   Apache OpenWhisk Controller component will mark invoker target as unavailable
                final Queue<IBufferizable> invocationQueue = bufferizables.stream()
                        .filter(b -> isInvokerHealthTestAction(b.getAction()))
                        .collect(toCollection(ArrayDeque::new));
                // remove invoker test action from input queue to be processed, if any
                if (!invocationQueue.isEmpty()) {
                    bufferizables.removeAll(invocationQueue);
                    invocationQueue.forEach(a -> LOG.trace("Activations with id {} did not acquire any resources.",
                            a.getActivationId()));
                }
                if (!bufferizables.isEmpty()) {
                    synchronized (mutex) {
                        // OPTIMIZE: if buffer is non empty and it is received an activation, all activation already
                        //   in the buffer can not be sent to invokers because of missing resources but new
                        //   activations received could be processed, iff require less resources;
                        //   so it is possible to process only new activations checking if can be sent to
                        //   invokers and, if false, add them to the buffer
                        // insert all received elements in the buffer,
                        //   reorder the buffer using selected policy
                        buffering(bufferizables);

                        // set containing invokers associated with activations just received
                        final Set<String> invokersWithActivations = bufferizables.stream()
                                .map(ISchedulable::getTargetInvoker)
                                .collect(toSet());
                        // remove from buffer all activations that can be processed on invokers
                        //   (so, invoker has sufficient resources to process the activations)
                        // Note: if new activation is received and can not be acquired resources on its
                        //   invoker target (invoker target selected by Controller component), there is no point
                        //   to check for others invokers because of Controller component, using hashing algorithm,
                        //   has yet tried to acquire resources on others invokers, without success, so it has
                        //   assigned randomly the invoker target, since system is overloaded
                        invocationQueue.addAll(pollAndAcquireResourcesForAllFlattened(
                                invokersWithActivations));
                    }
                }
                // send activations
                if (!invocationQueue.isEmpty()) send(invocationQueue);
            }
        } else if (stream.equals(COMPLETION_STREAM)) {
            final Collection<? extends Completion> completions = data.stream()
                    .filter(Completion.class::isInstance)
                    .map(Completion.class::cast)
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[CMP] - Processing {} completion objects (over {} received).",
                    completions.size(), data.size());

            if (completions.size() > 0) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
                synchronized (mutex) {
                    // contains id of invokers that have processed at least one completion
                    final Set<String> invokersWithCompletions = new HashSet<>(invokersMap.size());

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
                        if (activationId == null) {
                            LOG.warn("Completion received does not have valid activationId.");
                            continue;
                        }

                        final Invoker invoker = invokersMap.get(invokerTarget);
                        // there is no reference to this invoker in the system
                        if (invoker == null) continue;

                        final long activationsCountBeforeRelease = invoker.getActivationsCount();
                        // release resources associated with this completion (even if invoker is not healthy)
                        invoker.release(activationId);
                        // check if activation is effectively released
                        // activations that do not need to release resource are invokerHealthTestAction
                        if (activationsCountBeforeRelease - invoker.getActivationsCount() <= 0) {
                            LOG.trace("Activation with id {} did not release any resources.", activationId);
                            continue;
                        }

                        // add invoker to the set of invokers that have processed at least one completion
                        // note that, if n completions have been received, it is not sufficient check for
                        //   first n buffered activations because it is possible that one of the completion processed
                        //   released enough resources for m buffered activations
                        invokersWithCompletions.add(invokerTarget);
                    }

                    // second, for all invokers that have produced at least one completion,
                    //   check if there is at least one buffered activation that can be scheduled on it
                    //   (so, if it has necessary resources)
                    // Note: changing invoker target, that is, sending the activation to an invoker other than
                    //   the one chosen by the Controller component should not be a problem since the topic
                    //   target where the completion should be sent is specified in the rootControllerIndex field
                    //   of the activation record so, changing invoker target should not have impact in that sense;
                    //   also note that all Load Balancers (component of Controller) manages a portion of resources
                    //   of each invoker, so every invokers could publish on each topic associated to the controllers
                    //   ('completedN' topics).
                    //   The only problem could be related to the "instance" field of the activations records published
                    //   on the 'completedN' topics associated with Controllers, since e.g. a Controller expects
                    //   activation to come from Invoker0 instead of Invoker1.
                    if (!invokersWithCompletions.isEmpty())
                        invocationQueue.addAll(
                                pollAndAcquireResourcesForAllFlattened(invokersWithCompletions));
                }
                // send activations
                if (!invocationQueue.isEmpty()) send(invocationQueue);
            }
        } else if (stream.equals(HEALTH_STREAM)) {
            // TODO: manage case when an invoker get updated with more/less memory
            final Set<? extends Health> heartbeats = data.stream()
                    .filter(Health.class::isInstance)
                    .map(Health.class::cast)
                    // get only unique hearth-beats messages using Set data structure
                    .collect(toSet());
            /*LOG.trace("[HLT] - Processing {} uniques hearth-beats objects (over {} received).",
                    heartbeats.size(), data.size());*/

            if (heartbeats.size() > 0) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
                synchronized (mutex) {
                    // contains id of invokers that have turned healthy
                    final Set<String> invokersTurnedHealthy = new HashSet<>(invokersMap.size());

                    for (final Health health : heartbeats) {
                        final String invokerTarget = getInvokerTargetFrom(health.getInstance());
                        Invoker invoker = invokersMap.get(invokerTarget);

                        // invoker has not yet registered in the system
                        if (invoker == null) {
                            final Invoker newInvoker = new Invoker(invokerTarget, getUserMemoryFrom(health.getInstance()));
                            // register new invoker
                            invokersMap.put(invokerTarget, newInvoker);
                            invoker = newInvoker;
                            LOG.trace("New invoker registered in the system: {}.", invokerTarget);
                        }

                        // if invoker was already healthy, update its timestamp
                        if (invoker.isHealthy()) {
                            invoker.setLastCheck(Instant.now().toEpochMilli());
                            // invoker has become healthy
                        } else {
                            // upon receiving hearth-beat from invoker, mark that invoker as healthy
                            invoker.updateState(HEALTHY, Instant.now().toEpochMilli());
                            LOG.trace("Invoker {} marked as {}.", invokerTarget, invoker.getState());
                            // add invoker to the set of invokers that have turned healthy
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
            }
        } else {
            LOG.trace("Unable to manage data from stream {}.", stream.toString());
        }
    }

    @Override
    public void shutdown() {
        synchronized (mutex) {
            LOG.trace("Closing {} completion kafka consumers.", controllerCompletionConsumerMap.size());
            controllerCompletionConsumerMap.values().forEach(CompletionKafkaConsumerMock::close);
        }
        schedulerExecutors.shutdown();
        schedulerPeriodicExecutors.shutdown();
        LOG.trace("{} shutdown.", this.getClass().getSimpleName());
    }

    private void buffering(@Nonnull Collection<IBufferizable> bufferizables) {
        checkNotNull(bufferizables, "Input collection can not be null.");
        final Map<String, Collection<IBufferizable>> invokerCollectionMap = bufferizables.stream()
                .collect(groupingBy(IBufferizable::getTargetInvoker, toCollection(ArrayList::new)));
        buffering(invokerCollectionMap);
    }

    private void buffering(@Nonnull Map<String, Collection<IBufferizable>> invokerBufferizableMap) {
        checkNotNull(invokerBufferizableMap, "Input map can not be null.");
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

    private @Nonnull Queue<IBufferizable> pollAndAcquireResourcesForAllFlattened(@Nonnull Set<String> invokers) {
        final Map<String, Queue<IBufferizable>> invokerQueueMap = pollAndAcquireResourcesForAll(invokers);
        return invokerQueueMap.values().stream()
                .flatMap(Queue::stream)
                .collect(Collectors.toCollection(ArrayDeque::new));
    }

    private @Nonnull Map<String, Queue<IBufferizable>> pollAndAcquireResourcesForAll(@Nonnull Set<String> invokers) {
        checkNotNull(invokers, "Invokers set can not be null.");
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

                    // if count of activations to select have been reached, break iteration
                    if (count <= 0) break;
                    // checks if current buffered activation can be submitted
                    // note that it is not sufficient break iteration if can not be acquired
                    //   memory and concurrency on current activation because it is possible that
                    //   there is another activation in the buffered queue with requirements
                    //   to be scheduled on current invoker
                    if (!invoker.tryAcquireMemoryAndConcurrency(activation)) continue;

                    // add to invocation queue
                    invocationQueue.add(activation);
                    // remove from buffer
                    bufferIterator.remove();
                    // reduce activations numbers to send
                    --count;
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
                                entry.getValue().size() + " activations");
                    }
                })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        final Map<String, String> invokerResourcesTrace = invokersMap.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(
                        entry.getKey(), "(" + entry.getValue().getActivationsCount() + " actv | " +
                        entry.getValue().getMemory() + " MiB remaining)"))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        LOG.trace("Scheduling - {}.", invokerQueueTrace);
        LOG.trace("Resources - {}.", invokerResourcesTrace);
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
        if (invokersMap.isEmpty()) return;
        synchronized (mutex) {
            for (final Invoker invoker : invokersMap.values()) {
                // invoker already in offline state, so check next invoker
                if (invoker.getState() == OFFLINE) continue;
                // if invoker has not sent hearth-beat in delta, mark it as offline
                if (now - invoker.getLastCheck() > offlineCheck) {
                    // timestamp of the last update will not be updated
                    invoker.updateState(OFFLINE);
                    invoker.removeAllContainers();
                    LOG.trace("Invoker {} marked as {}.", invoker.getInvokerName(), invoker.getState());
                    // when invoker is marked as offline, release resources associated with it
                    invokerBufferMap.remove(invoker.getInvokerName());
                    // continue because if invoker is offline is also unhealthy
                    continue;
                }

                // invoker already in unhealthy state, so check next invoker
                if (invoker.getState() == UNHEALTHY) continue;
                // if invoker has not sent hearth-beat in delta, mark it as unhealthy
                if (now - invoker.getLastCheck() > healthCheck) {
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

    private @Nonnull CompletionKafkaConsumerMock createCompletionConsumerFrom(@Nonnull String instance) {
        checkNotNull(instance, "Instance can not be null.");
        final String topic = String.format(TEMPLATE_COMPLETION_TOPIC, instance);
        LOG.trace("Creating new completion Kafka consumer for topic: {}.", topic);
        final CompletionKafkaConsumerMock completionKafkaConsumer = new CompletionKafkaConsumerMock(
                List.of(topic), kafkaConsumerProperties, 50
        );
        register(List.of(completionKafkaConsumer));
        completionKafkaConsumer.register(List.of(this));
        Objects.requireNonNull(schedulerExecutors.networkIO()).submit(completionKafkaConsumer);
        return completionKafkaConsumer;
    }

    private void schedulePeriodicActivities() {
        // release activations too old, assuming there was an error sending completion
        // check performed in this branch to reduce its frequency
        //   (should not be performed in completion branch in case completion
        //   Kafka consumers fail)
        Objects.requireNonNull(schedulerPeriodicExecutors.computation()).scheduleWithFixedDelay(
                () -> releaseActivationsOlderThan(runningActivationTimeLimitMs),
                0L,
                runningActivationTimeLimitMs,
                TimeUnit.MILLISECONDS
        );

        // check if some invoker turned unhealthy or offline
        Objects.requireNonNull(schedulerPeriodicExecutors.computation()).scheduleWithFixedDelay(
                () -> healthCheck(healthCheckTimeLimitMs, offlineTimeLimitMs),
                0L,
                healthCheckTimeLimitMs,
                TimeUnit.MILLISECONDS
        );
    }

    public static @Nonnull String getInvokerTargetFrom(@Nonnull InvokerInstance invokerInstance) {
        checkNotNull(invokerInstance, "Instance can not be null.");
        return invokerInstance.getInstanceType().getName() + invokerInstance.getInstance();
    }

    public static int getInstanceFrom(@Nonnull String invokerTarget) {
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
     * @param invokerInstance
     * @return
     */
    public static long getUserMemoryFrom(@Nonnull InvokerInstance invokerInstance) {
        checkNotNull(invokerInstance, "Instance can not be null.");
        return Long.parseLong(invokerInstance.getUserMemory().split(" ")[0]) / (1024L * 1024L);
    }

    public void setKafkaBootstrapServers(@Nonnull String kafkaBootstrapServers) {
        this.kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    }

    public long getHealthCheckTimeLimitMs() {
        return healthCheckTimeLimitMs;
    }

    public long getOfflineTimeLimitMs() {
        return offlineTimeLimitMs;
    }

    public long getRunningActivationTimeLimitMs() {
        return runningActivationTimeLimitMs;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

}