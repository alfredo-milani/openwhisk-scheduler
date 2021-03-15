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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
    // allow max 4 Apache OpeWhisk Controller instances
    public static final int THREAD_COUNT_KAFKA_COMPLETED_CONSUMERS = 4;
    public static final int THREAD_COUNT_PERIODIC_ACTIVITIES = 2;
    public static final int THREAD_COUNT_KAFKA_PRODUCERS = 5;
    public static final long HEALTH_CHECK_TIME_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long OFFLINE_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);
    public static final long RUNNING_ACTIVATION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(15);
    public static final int BUFFER_SIZE = 1000;
    public static final float OVERLOAD_RATIO = 1;

    // time after which an invoker is marked as unhealthy
    private long healthCheckTimeLimitMs = HEALTH_CHECK_TIME_MS;
    // time after which an invoker is marked as offline
    private long offlineTimeLimitMs = OFFLINE_TIME_LIMIT_MS;
    // time after which running activations on invokers are removed
    //   (security measure to not block invoker in case completion message is missing)
    private long runningActivationTimeLimitMs = RUNNING_ACTIVATION_TIME_LIMIT_MS;
    // over-allocate invoker's resource to let activations queue up on invoker
    private float overloadRatio = OVERLOAD_RATIO;
    private final SchedulerExecutors schedulerExecutors = new SchedulerExecutors(
            THREAD_COUNT_KAFKA_COMPLETED_CONSUMERS + THREAD_COUNT_KAFKA_PRODUCERS,
            0
    );
    private final SchedulerPeriodicExecutors schedulerPeriodicExecutors = new SchedulerPeriodicExecutors(
            0,
            THREAD_COUNT_PERIODIC_ACTIVITIES
    );
    private final Object mutex = new Object();
    // <targetInvoker, Invoker>
    // invoker's kafka topic name used as unique id for invoker
    // Note: must be externally synchronized to safely access Invoker entities
    private final Map<String, Invoker> invokersMap = new LinkedHashMap<>(24);
    // activations buffer
    private final ActivationBuffer activationsBuffer;
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
    private final IPolicy policy;
    private final IProducer producer;

    private final class ActivationBuffer {
        public static final int DEFAULT_SIZE = 1000;

        private final IPolicy policy;
        private int capacity;
        private boolean sorted;
        private ArrayDeque<IBufferizable> buffer;

        ActivationBuffer(@Nonnull final IPolicy policy) {
            this(policy, DEFAULT_SIZE);
        }

        ActivationBuffer(@Nonnull final IPolicy policy, final int capacity) {
            checkNotNull(policy, "Policy can not be null.");
            this.policy = policy;
            this.capacity = capacity;
            this.sorted = true;
            this.buffer = new ArrayDeque<>(capacity);
        }

        public int getSize() {
            return buffer.size();
        }

        public boolean isSorted() {
            return sorted;
        }

        /**
         * Return buffer reference: no shallow/deep copy.
         * Before returning the buffer, if it is not sorted, sort it using
         * specified {@link IPolicy}.
         *
         * @return sorted buffer.
         */
        public @Nonnull Queue<IBufferizable> getSortedBuffer() {
            if (!sorted) {
                buffer = (ArrayDeque<IBufferizable>) policy.apply(buffer);
                sorted = true;
            }
            return buffer;
        }

        public @Nonnull Queue<IBufferizable> getBuffer() {
            return buffer;
        }

        /**
         * Adds all of the elements in the specified collection to this collection
         * (optional operation).  The behavior of this operation is undefined if
         * the specified collection is modified while the operation is in progress.
         * (This implies that the behavior of this call is undefined if the
         * specified collection is this collection, and this collection is
         * nonempty.)
         * If the number of elements of this collection plus specified one are greater
         * than {@link #capacity}, last elements of the buffer will be removed.
         *
         * @param elements collection containing elements to be added to this collection
         * @return {@code true} if this collection changed as a result of the call
         * @throws UnsupportedOperationException if the {@code addAll} operation
         *         is not supported by this collection
         * @throws ClassCastException if the class of an element of the specified
         *         collection prevents it from being added to this collection
         * @throws NullPointerException if the specified collection contains a
         *         null element and this collection does not permit null elements,
         *         or if the specified collection is null
         * @throws IllegalArgumentException if some property of an element of the
         *         specified collection prevents it from being added to this
         *         collection
         * @throws IllegalStateException if not all the elements can be added at
         *         this time due to insertion restrictions
         * @see ArrayDeque#add(Object)
         */
        public boolean addAll(@Nonnull final Collection<IBufferizable> elements) {
            if (elements.isEmpty()) return false;
            // remove old activations if buffer exceed its max size
            final int totalDemand = buffer.size() + elements.size();
            if (totalDemand > capacity) {
                if (!sorted) buffer = (ArrayDeque<IBufferizable>) policy.apply(buffer);
                int toRemove = totalDemand - capacity;
                for (int i = 0; i < toRemove; ++i) buffer.removeLast();
                LOG.debug("Reached buffer limit ({}) - discarding last {} activations.", capacity, toRemove);
            }

            sorted = false;
            return buffer.addAll(elements);
        }

        public boolean removeAll(@Nonnull final Collection<IBufferizable> elements) {
            if (elements.isEmpty()) return false;
            return buffer.removeAll(elements);
        }

        public int getCapacity() {
            return capacity;
        }

        public void setCapacity(int capacity) {
            checkArgument(capacity > 0, "Capacity must be > 0.");
            this.capacity = capacity;
        }
    }

    public BufferedSchedulerMock(@Nonnull final IPolicy policy, @Nonnull final IProducer producer) {
        checkNotNull(policy, "Policy can not be null.");
        checkNotNull(producer, "Producer can not be null.");

        this.policy = policy;
        this.producer = producer;
        this.activationsBuffer = new ActivationBuffer(policy);

        // OPTIMIZE: to assign scheduled activities frequency dynamically create
        //   new BufferedScheduler constructor with required parameters
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
        if (stream.equals(ACTIVATION_STREAM)) {
            final Collection<IBufferizable> newActivations = data.stream()
                    .filter(IBufferizable.class::isInstance)
                    .map(IBufferizable.class::cast)
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[ACT] - Processing {} activations objects (over {} received).",
                    newActivations.size(), data.size());

            if (!newActivations.isEmpty()) {
                // create new kafka completion consumer, if needed
                // Note: for now only one thread modify completionKafkaConsumersMap,
                //   so there is no need for locking
                updateCompletionsConsumersFrom(newActivations);

                // invocation queue
                // invoker health test action must always be processed, otherwise
                //   Apache OpenWhisk Controller component will mark invoker target as unavailable
                final Queue<IBufferizable> invocationQueue = newActivations.stream()
                        .filter(b -> isInvokerHealthTestAction(b.getAction()))
                        .collect(toCollection(ArrayDeque::new));
                // remove invoker test action from input queue to be processed, if any
                if (!invocationQueue.isEmpty()) {
                    newActivations.removeAll(invocationQueue);
                    for (final IBufferizable invokerHealthTestAction : invocationQueue)
                        LOG.trace("Activations with id {} did not acquire any resources.",
                                invokerHealthTestAction.getActivationId());
                }
                if (!newActivations.isEmpty()) {
                    synchronized (mutex) {
                        // try to schedule new activations (acquiring resources on invokers)
                        final Queue<IBufferizable> scheduledActivations = schedule(
                                // apply selected policy to new activations before scheduling them
                                (Queue<IBufferizable>) policy.apply(newActivations),
                                new ArrayList<>(invokersMap.values())
                        );
                        // add all scheduled activations to invocation queue
                        invocationQueue.addAll(scheduledActivations);
                        // remove all scheduled activations
                        newActivations.removeAll(scheduledActivations);
                        // buffering remaining activations
                        activationsBuffer.addAll(newActivations);

                        // log trace
                        if (LOG.getLevel().equals(Level.TRACE)) {
                            schedulingStats(scheduledActivations);
                            resourcesStats(invokersMap);
                            bufferStats(activationsBuffer.getBuffer());
                        }
                    }
                }
                // send activations
                if (!invocationQueue.isEmpty())
                    schedulerExecutors.networkIO().execute(() -> send(producer, invocationQueue));
            }
        } else if (stream.equals(COMPLETION_STREAM)) {
            final Collection<Completion> completions = data.stream()
                    .filter(Completion.class::isInstance)
                    .map(Completion.class::cast)
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[CMP] - Processing {} completion objects (over {} received).",
                    completions.size(), data.size());

            if (!completions.isEmpty()) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue;
                synchronized (mutex) {
                    // update policy's state, if needed
                    policy.update(completions);
                    // contains id of invokers that have processed at least one completion
                    final List<Invoker> invokersWithCompletions = processCompletions(completions, invokersMap);
                    // if no valid completion has been received, release lock immediately
                    if (invokersWithCompletions.isEmpty()) return;

                    // for all invokers that have produced at least one completion,
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
                    // WARNING: using this new approach makes Controller state of invokers not reliable, since
                    //   here, the Scheduler assigns activations to a different invoker target, invoker target not
                    //   chosen by Controller
                    //   Example: utilizzando questa versione (v0.0.21) la vista che ha lo Scheduler dello stato degli
                    //     invokers e quella che il Controller non sono coerenti perché: se il buffer è non vuoto
                    //     e uno degli invokers genera una 'completion', allora lo Scheduler vede se nel buffer
                    //     complessivo del sistema (cioè tra tutte la activations presenti nel sistema, senza
                    //     considerare le activations divise per ogni invoker) c'è una activation che può essere
                    //     processata da quell'invoker, anche se effettivamente non è lui l'invoker target per quella
                    //     specifica activation.
                    //     Questo approccio porta uno svantaggio: perdita di coesione tra la vista dello stato del
                    //     Controller e qualla dello Scheduler. Esempio: ci sono 2 invokers e sono saturi; inizialmente
                    //     lo stato dello Scheduler e del Controller sono coerenti; invoker1 processa una 'completion',
                    //     lo Scheduler prende una activation presente nella coda dell'invoker0 e la invia all'invoker1 e
                    //     aggiorna il proprio stato; il Controller non è a conoscenza dell'aggiornamento di stato e per
                    //     lui, l'invoker1 ha 1 slot di memoria disponibile, mentre per lo Scheduler, l'invoker1 non ha
                    //     slots di memoria disponibili.
                    //     Cosa comporta questo? Questo porta alla perdita di efficienza del principio di località
                    //     implementato nel Controller. Continuazione esempio di cui sopra: arriva una activation nel
                    //     sistema:
                    //     1. sa la activation ha come home invoker l'invoker0: invoker0 è saturo per assunzione
                    //     iniziale, quindi viene selezionato l'invoker1 (vedi 2.)
                    //     2. la activation ha come home invoker l'invoker1: per il Controller, invoker1 non è saturo
                    //       2.1 invoker1 può accettare la nuova activation: il Controller invia ad esso la nuova
                    //       activation
                    //       2.2 invoker1 non può accettare la nuova activation: il Controller sceglie un invoker
                    //       casuale ed invia ad esso la nuova activation
                    //       NOTA: il Controller può acquisire risorse sugli invokers (sullo stato che esso ha degli
                    //         invokers) in modo forzato; questo viene fatto per tenere in considerazione le code che
                    //         possono formarsi sugli invokers nel caso il sistema risulti saturo
                    //     Nel caso 2.1, la activation arriva sullo Scheduler ma non può essere eseguita dall'invoker1
                    //     e viene messa in coda, anche se, per il Controller poteva essere eseguita
                    //     Un altro caso da considerare è quello relativo ad un 'completamento': invoker0 e invoker1
                    //     sono saturi e invoker0 ha 1 activation in coda; inizialmente lo stato di Controller e
                    //     Scheduler sono allineati; invoker1 genera un 'completamento', il Controller aggiorna lo
                    //     stato, lo Scheduler aggiorna lo stato e invia ad esso la activation che era in coda su
                    //     invoker0; invoker1 genera un 'completamento' per tale activation; il Controller aggiorna lo
                    //     stato di invoker1 in modo errato: decrementa risorse che non erano state acquisite (nota:
                    //     la activation viene correttamente rimossa dallo stato del Controller).
                    //   Quindi, sia in caso di ricezione di nuove activations che in caso di completions, lo stato
                    //   del Controller risulta corrotto.
                    //   Nota: se non si forma coda sullo Scheduler, lo stato dello Scheduler e quello del Contoller
                    //   sono allineati e quindi il sistema funziona come di norma (sfruttando cioè il principio di
                    //   località implementato nel Controller)
                    invocationQueue = schedule(
                            activationsBuffer.getSortedBuffer(),
                            invokersWithCompletions
                    );
                    // remove all scheduled activations from buffer
                    activationsBuffer.removeAll(invocationQueue);

                    // log trace
                    if (!invokersWithCompletions.isEmpty() && LOG.getLevel().equals(Level.TRACE)) {
                        schedulingStats(invocationQueue);
                        resourcesStats(invokersMap);
                        bufferStats(activationsBuffer.getBuffer());
                    }
                }
                // send activations
                if (!invocationQueue.isEmpty())
                    schedulerExecutors.networkIO().execute(() -> send(producer, invocationQueue));
            }
        } else if (stream.equals(HEALTH_STREAM)) {
            // TODO: manage case when an invoker get updated with more/less memory
            final Set<Health> heartbeats = data.stream()
                    .filter(Health.class::isInstance)
                    .map(Health.class::cast)
                    // get only unique hearth-beats messages using Set data structure
                    .collect(toSet());
            /*LOG.trace("[HLT] - Processing {} uniques hearth-beats objects (over {} received).",
                    heartbeats.size(), data.size());*/

            if (!heartbeats.isEmpty()) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue;
                synchronized (mutex) {
                    // contains id of invokers that have turned healthy
                    final List<Invoker> invokersTurnedHealthy = processHearthBeats(
                            new ArrayList<>(heartbeats),
                            invokersMap
                    );
                    // since heart-beats arrives every seconds from invokers,
                    //   if there is no invoker turned healthy, release lock immediately
                    if (invokersTurnedHealthy.isEmpty()) return;

                    // schedule activations to all invokers turned healthy
                    invocationQueue = schedule(
                            activationsBuffer.getSortedBuffer(),
                            invokersTurnedHealthy
                    );
                    // remove all scheduled activations from buffer
                    activationsBuffer.removeAll(invocationQueue);

                    // log trace
                    if (!invocationQueue.isEmpty() && LOG.getLevel().equals(Level.TRACE)) {
                        schedulingStats(invocationQueue);
                        resourcesStats(invokersMap);
                        bufferStats(activationsBuffer.getBuffer());
                    }
                }
                // send activations
                if (!invocationQueue.isEmpty())
                    schedulerExecutors.networkIO().execute(() -> send(producer, invocationQueue));
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
        LOG.info("{} shutdown.", this.getClass().getSimpleName());
    }

    private void updateCompletionsConsumersFrom(@Nonnull final Collection<IBufferizable> newActivations) {
        // filter unique controller instance from activation record and check if there is yet
        //   a completion kafka consumer, otherwise create it
        // for now, created completion kafka consumers run indefinitely, without a mechanism to check
        //   if there are still producer which publish on that topics, but it's ok
        final Set<RootControllerIndex> controllers = newActivations.stream()
                .map(IBufferizable::getRootControllerIndex)
                .filter(Objects::nonNull)
                .collect(toSet());
        // create new consumer for controller instance if it is not yet present
        controllers.removeAll(controllerCompletionConsumerMap.keySet());
        for (final RootControllerIndex controller : controllers) {
            controllerCompletionConsumerMap.put(controller,
                    createCompletionConsumerFrom(controller.getAsString()));
        }
    }

    private @Nonnull List<Invoker> processHearthBeats(@Nonnull final Collection<Health> heartBeats,
                                                      @Nonnull final Map<String, Invoker> invokersMap) {
        final List<Invoker> invokersTurnedHealthy = new ArrayList<>(invokersMap.size());
        if (heartBeats.isEmpty()) return invokersTurnedHealthy;

        for (final Health health : heartBeats) {
            final String invokerTarget = getInvokerTargetFrom(health.getInstance());
            Invoker invoker = invokersMap.get(invokerTarget);

            // invoker has not yet registered in the system
            if (invoker == null) {
                final Invoker newInvoker = new Invoker(
                        invokerTarget,
                        (long) (overloadRatio * getUserMemoryFrom(health.getInstance()))
                );
                // register new invoker
                invokersMap.put(invokerTarget, newInvoker);
                invoker = newInvoker;
                LOG.debug("New invoker registered in the system: {}.", invokerTarget);
            }

            // if invoker was already healthy, update its timestamp
            if (invoker.isHealthy()) {
                invoker.setLastCheck(Instant.now().toEpochMilli());
                // invoker has become healthy
            } else {
                // upon receiving hearth-beat from invoker, mark that invoker as healthy
                invoker.updateState(HEALTHY, Instant.now().toEpochMilli());
                LOG.debug("Invoker {} marked as {}.", invokerTarget, invoker.getState());
                // add invoker to the set of invokers that have turned healthy
                invokersTurnedHealthy.add(invoker);
            }
        }

        return invokersTurnedHealthy;
    }

    private @Nonnull List<Invoker> processCompletions(@Nonnull final Collection<Completion> completions,
                                                      @Nonnull final Map<String, Invoker> invokersMap) {
        final List<Invoker> invokersWithCompletions = new ArrayList<>(invokersMap.size());
        if (completions.isEmpty()) return invokersWithCompletions;

        // free resources for all received completions
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
            //   released enough resources for m, with m >= n, buffered activations
            invokersWithCompletions.add(invoker);
        }

        return invokersWithCompletions;
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
                producer.produce(
                        schedulable.getTargetInvoker(),
                        (IConsumable) schedulable.with(schedulingTermination)
                );
            }
        }
    }

    private void healthCheck(long healthCheck, long offlineCheck) {
        if (invokersMap.isEmpty()) return;
        long now = Instant.now().toEpochMilli();
        synchronized (mutex) {
            for (final Invoker invoker : invokersMap.values()) {
                // if invoker has not sent hearth-beat in delta, mark it as offline
                if (invoker.getState() != OFFLINE && now - invoker.getLastCheck() > offlineCheck) {
                    // timestamp of the last update will not be updated
                    invoker.updateState(OFFLINE);
                    invoker.removeAll();
                    LOG.trace("Invoker {} marked as {}.", invoker.getInvokerName(), invoker.getState());
                    // continue because if invoker is offline is also unhealthy
                    continue;
                }

                // if invoker has not sent hearth-beat in delta, mark it as unhealthy
                if (invoker.getState() != UNHEALTHY && now - invoker.getLastCheck() > healthCheck) {
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
        if (invokersMap.isEmpty()) return;
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
        final String topic = String.format(TEMPLATE_COMPLETION_TOPIC, instance);
        LOG.trace("Creating new completion Kafka consumer for topic: {}.", topic);
        final CompletionKafkaConsumerMock completionKafkaConsumer = new CompletionKafkaConsumerMock(
                List.of(topic), kafkaConsumerProperties, 50
        );
        register(List.of(completionKafkaConsumer));
        completionKafkaConsumer.register(List.of(this));
        schedulerExecutors.networkIO().submit(completionKafkaConsumer);
        return completionKafkaConsumer;
    }

    private void schedulePeriodicActivities() {
        // release activations too old, assuming there was an error sending completion
        schedulerPeriodicExecutors.computation().scheduleWithFixedDelay(
                () -> releaseActivationsOlderThan(runningActivationTimeLimitMs),
                runningActivationTimeLimitMs,
                runningActivationTimeLimitMs,
                TimeUnit.MILLISECONDS
        );

        // check if some invoker turned unhealthy or offline
        schedulerPeriodicExecutors.computation().scheduleWithFixedDelay(
                () -> healthCheck(healthCheckTimeLimitMs, offlineTimeLimitMs),
                healthCheckTimeLimitMs,
                healthCheckTimeLimitMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Try acquire resource for all {@link ISchedulable} received in input.
     *
     * @param activations
     * @param invokers
     * @return all {@link ISchedulable} objects for which it was possible to acquire the necessary resources.
     */
    private @Nonnull Queue<IBufferizable> schedule(@Nonnull final Queue<IBufferizable> activations,
                                                   @Nonnull final List<Invoker> invokers) {
        final Queue<IBufferizable> activationsScheduled = new ArrayDeque<>(activations.size());
        if (activations.isEmpty() || invokers.isEmpty()) return activationsScheduled;

        final int invokersSize = invokers.size();
        final List<Integer> stepSizes = pairwiseCoprimeNumbersUntil(invokersSize);
        for (final IBufferizable activation : activations) {
            if (activation.getAction() == null) continue;

            final int hash = generateHashFrom(activation.getAction());
            final int homeInvoker = hash % invokersSize;
            final int stepSize = stepSizes.get(hash % stepSizes.size());

            int invokerIndex = homeInvoker;
            for (int i = 0; i < invokersSize; ++i) {
                final Invoker invoker = invokers.get(invokerIndex);
                // TODO: consider if there are changes to be done to update invoker pool
                //   correctly as in ShardingContainerPoolBalancer#updateInvokers(IndexedSeq[InvokerHealth])
                if (invoker.isHealthy() && invoker.tryAcquireMemoryAndConcurrency(activation)) {
                    // do not rely Apache OpenWhisk Controller selected invoker,
                    //   because its state may be corrupted by Scheduler's buffering mechanism
                    activationsScheduled.add(activation.with(invoker.getInvokerName()));
                    break;
                } else {
                    invokerIndex = (invokerIndex + stepSize) % invokersSize;
                }
            }
        }

        return activationsScheduled;
    }

    private void schedulingStats(@Nonnull final Queue<IBufferizable> scheduling) {
        final Map<String, String> activationsScheduledTrace = scheduling.stream()
                .collect(groupingBy(IBufferizable::getTargetInvoker, toUnmodifiableList()))
                .entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().size() + " actv"));
        LOG.trace("Scheduling - {}.", activationsScheduledTrace);
    }

    private void resourcesStats(@Nonnull final Map<String, Invoker> resources) {
        final Map<String, String> invokersResourcesTrace = resources.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(
                        entry.getKey(), "(" + entry.getValue().getActivationsCount() + " actv | " +
                        entry.getValue().getMemory() + " MiB remaining)"))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        LOG.trace("Resources - {}.", invokersResourcesTrace);
    }

    private void bufferStats(@Nonnull final Queue<IBufferizable> buffer) {
        /*final Map<String, Integer> activationsBufferTrace = buffer.stream()
                .collect(groupingBy(IBufferizable::getTargetInvoker, toUnmodifiableList()))
                .entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
        LOG.trace("Buffer - {}.", activationsBufferTrace);*/

        LOG.trace("Buffer - {buffer={}}.", buffer.size());
    }

    /**
     * Generates a hash based on the string representation of {@link Action#getPath()} and {@link Action#getName()}.
     *
     * @param action
     * @return
     */
    private static int generateHashFrom(@Nonnull final Action action) {
        return Math.abs(action.getPath().hashCode() ^ action.getName().hashCode());
    }

    /**
     * Euclidean algorithm to determine the greatest-common-divisor.
     *
     * @param a
     * @param b
     * @return
     */
    private static int gcd(int a, int b) {
        if (b == 0) return a;
        else return gcd(b, a % b);
    }

    /**
     * Returns pairwise coprime numbers until n.
     *
     * @param n
     * @return
     */
    private static @Nonnull List<Integer> pairwiseCoprimeNumbersUntil(int n) {
        final ArrayList<Integer> primes = new ArrayList<>();
        IntStream.rangeClosed(1, n).forEach(i -> {
            if (gcd(i, n) == 1 && primes.stream().allMatch(j -> gcd(j, i) == 1)) primes.add(i);
        });
        return primes;
    }

    public static @Nonnull String getInvokerTargetFrom(@Nonnull final InvokerInstance invokerInstance) {
        return invokerInstance.getInstanceType().getName() + invokerInstance.getInstance();
    }

    public static boolean isInvokerHealthTestAction(@Nullable final Action action) {
        return action != null && action.getPath().equals("whisk.system") &&
                action.getName().matches("^invokerHealthTestAction[0-9]+$");
    }

    /**
     * Return user-memory in MiB.
     *
     * @param invokerInstance
     * @return
     */
    public static long getUserMemoryFrom(@Nonnull InvokerInstance invokerInstance) {
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

    public int getBufferSize() {
        return activationsBuffer.getSize();
    }

    public void setBufferSize(int bufferSize) {
        this.activationsBuffer.setCapacity(bufferSize);
    }

    public float getOverloadRatio() {
        return overloadRatio;
    }

    public void setOverloadRatio(float overloadRatio) {
        this.overloadRatio = overloadRatio;
    }

}