package it.uniroma2.faas.openwhisk.scheduler.scheduler;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IProducer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ActivationKafkaConsumer.ACTIVATION_STREAM;
import static it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer.COMPLETION_STREAM;
import static java.util.stream.Collectors.*;

public class BufferedSchedulerv0_3_18t extends Scheduler {

    private final static Logger LOG = LogManager.getLogger(BufferedScheduler.class.getCanonicalName());

    public static final String TEMPLATE_COMPLETION_TOPIC = "completed%s";
    public static final int THREAD_COUNT = 3;
    public static final long HEALTH_CHECK_TIME_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long OFFLINE_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(5);
    public static final long RUNNING_ACTIVATION_TIME_LIMIT_MS = TimeUnit.MINUTES.toMillis(15);
    public static final int MAX_BUFFER_SIZE = 1000;

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
    // boolean represents if corresponding invoker's buffer is sorted or not
    private final Map<String, Boolean> invokerBufferSortedMap = new HashMap<>(24);
    // <controller, CompletionKafkaConsumer>
    // contains all completion Kafka consumer associated with controllers
    private final Map<RootControllerIndex, CompletionKafkaConsumer> controllerCompletionConsumerMap =
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

    public BufferedSchedulerv0_3_18t(@Nonnull IPolicy policy, @Nonnull IProducer producer) {
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
    @SuppressWarnings("unchecked")
    @Override
    public void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data) {
        if (stream.equals(ACTIVATION_STREAM)) {
            Collection<IBufferizable> newActivations = data.stream()
                    .filter(IBufferizable.class::isInstance)
                    .map(IBufferizable.class::cast)
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[ACT] - Processing {} activations objects (over {} received).",
                    newActivations.size(), data.size());

            if (newActivations.size() > 0) {
                // create new kafka completion consumer, if needed
                // Note: for now only one thread modify completionKafkaConsumersMap,
                //   so there is no need for locking
                updateCompletionsConsumersFrom(newActivations);

                // invocation queue
                // add all invoker test action activations without acquiring any resource
                // invoker health test action must always be processed, otherwise
                //   Apache OpenWhisk Controller component will mark invoker target as unavailable
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>(newActivations.size());
                newActivations.stream()
                        .filter(b -> isInvokerHealthTestAction(b.getAction()))
                        .forEach(invocationQueue::add);
                // remove invoker test action from input queue to be processed, if any
                if (!invocationQueue.isEmpty()) {
                    newActivations.removeAll(invocationQueue);
                    invocationQueue.forEach(a -> LOG.trace("Activations with id {} did not acquire any resources.",
                            a.getActivationId()));
                }
                if (!newActivations.isEmpty()) {
                    synchronized (mutex) {
                        // try to schedule new activations (acquiring resources on invokers)
                        final Queue<IBufferizable> scheduledActivations = schedule(
                                // apply selected policy to new activations before scheduling them
                                (Queue<IBufferizable>) policy.apply(newActivations),
                                invokersMap,
                                0
                        );
                        // remove all scheduled activations
                        newActivations.removeAll(scheduledActivations);
                        // buffering remaining activations
                        buffering(newActivations, invokerBufferMap, invokerBufferSortedMap);
                        // add all scheduled activations to invocation queue
                        invocationQueue.addAll(scheduledActivations);

                        // log trace
                        if (LOG.getLevel().equals(Level.TRACE)) {
                            schedulingStats(scheduledActivations);
                            resourcesStats(invokersMap);
                            bufferStats(invokerBufferMap);
                        }
                    }
                }
                // send activations
                if (!invocationQueue.isEmpty()) send(invocationQueue);
            }
        } else if (stream.equals(COMPLETION_STREAM)) {
            final Collection<Completion> completions = data.stream()
                    .filter(Completion.class::isInstance)
                    .map(Completion.class::cast)
                    .collect(toCollection(ArrayDeque::new));
            LOG.trace("[CMP] - Processing {} completion objects (over {} received).",
                    completions.size(), data.size());

            if (completions.size() > 0) {
                // invocation queue
                final Queue<IBufferizable> invocationQueue = new ArrayDeque<>();
                synchronized (mutex) {
                    // update policy's state, if needed
                    policy.update(completions);
                    // contains id of invokers that have processed at least one completion
                    final List<Invoker> invokersWithCompletions = processCompletions(completions, invokersMap);

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
                    for (final Invoker invoker : invokersWithCompletions) {
                        final String targetInvoker = invoker.getInvokerName();
                        Queue<IBufferizable> invokerBuffer = invokerBufferMap.get(targetInvoker);
                        // if there is no buffer for that invoker name, that invoker is
                        //   not yet registered in the system, so continue with iteration
                        if (invokerBuffer == null) continue;
                        // apply policy if buffer is not yet sorted
                        if (!invokerBufferSortedMap.get(targetInvoker)) {
                            invokerBufferMap.put(
                                    targetInvoker,
                                    invokerBuffer = (Queue<IBufferizable>) policy.apply(invokerBuffer)
                            );
                            invokerBufferSortedMap.put(targetInvoker, true);
                        }
                        // schedule activations
                        final Queue<IBufferizable> scheduledActivations = schedule(
                                invokerBuffer,
                                invokersMap,
                                0
                        );
                        // remove all scheduled activations from buffer
                        invokerBuffer.removeAll(scheduledActivations);
                        // add all scheduled activations to invocation queue
                        invocationQueue.addAll(scheduledActivations);

                        // log trace
                        if (!invokersWithCompletions.isEmpty() && LOG.getLevel().equals(Level.TRACE)) {
                            schedulingStats(scheduledActivations);
                            resourcesStats(invokersMap);
                            bufferStats(invokerBufferMap);
                        }
                    }
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
            controllerCompletionConsumerMap.values().forEach(CompletionKafkaConsumer::close);
        }
        schedulerExecutors.shutdown();
        schedulerPeriodicExecutors.shutdown();
        LOG.trace("{} shutdown.", this.getClass().getSimpleName());
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
        controllers.forEach(controller -> controllerCompletionConsumerMap.put(
                controller, createCompletionConsumerFrom(controller.getAsString())
        ));
    }

    // @SuppressWarnings("unchecked")
    private void buffering(@Nonnull final Collection<IBufferizable> newActivations,
                           @Nonnull final Map<String, Queue<IBufferizable>> invokerBufferMap,
                           @Nonnull final Map<String, Boolean> invokerBufferSortedMap) {
        if (newActivations.isEmpty()) return;

        final Map<String, Queue<IBufferizable>> invokerActivationMap = newActivations.stream()
                .collect(groupingBy(IBufferizable::getTargetInvoker, toCollection(ArrayDeque::new)));

        for (final Map.Entry<String, Queue<IBufferizable>> entry : invokerActivationMap.entrySet()) {
            final String targetInvoker = entry.getKey();
            final Queue<IBufferizable> newInvokerActivations = entry.getValue();

            Queue<IBufferizable> invokerBuffer = invokerBufferMap.get(targetInvoker);
            if (invokerBuffer == null) {
                invokerBufferMap.put(targetInvoker, invokerBuffer = new ArrayDeque<>());
            }

            // remove old activations if buffer exceed its max size
            final int total = invokerBuffer.size() + newInvokerActivations.size();
            if (total > maxBufferSize) {
                // creating new array dequeue to use #removeLast method
                final int toRemove = total - maxBufferSize;
                for (int i = 0; i < toRemove; ++i) ((ArrayDeque<IBufferizable>) invokerBuffer).removeLast();
                LOG.trace("Reached buffer limit ({}) - discarding last {} activations.", maxBufferSize, toRemove);
            }

            // add all new activations
            invokerBuffer.addAll(newInvokerActivations);
            invokerBufferSortedMap.put(targetInvoker, false);
            // invokerBufferMap.put(targetInvoker, (Queue<IBufferizable>) policy.apply(invokerBuffer));
        }
    }

    private @Nonnull Queue<IBufferizable> schedule(@Nonnull final Queue<IBufferizable> activations,
                                                   @Nonnull final Map<String, Invoker> invokers,
                                                   final int invokerBufferSize) {
        final Queue<IBufferizable> activationsScheduled = new ArrayDeque<>(activations.size());
        if (activations.isEmpty()) return activationsScheduled;

        final Map<String, Queue<IBufferizable>> invokerActivationMap = activations.stream()
                .collect(groupingBy(IBufferizable::getTargetInvoker, toCollection(ArrayDeque::new)));

        for (final Map.Entry<String, Queue<IBufferizable>> entry : invokerActivationMap.entrySet()) {
            final String targetInvoker = entry.getKey();
            final Queue<IBufferizable> newInvokerActivations = entry.getValue();
            Invoker invoker = invokers.get(targetInvoker);
            if (invoker == null) {
                // register new invoker
                invokers.put(targetInvoker, invoker = new Invoker(targetInvoker,
                        newInvokerActivations.peek().getUserMemory()));
                LOG.trace("New invoker registered in the system: {}.", targetInvoker);
            }

            Iterator<IBufferizable> newInvokerActivationsIterator = newInvokerActivations.iterator();
            while (newInvokerActivationsIterator.hasNext()) {
                final IBufferizable activation = newInvokerActivationsIterator.next();
                if (invoker.tryAcquireMemoryAndConcurrency(activation)) {
                    activationsScheduled.add(activation);
                    newInvokerActivationsIterator.remove();
                }
            }

            if (newInvokerActivations.isEmpty()) continue;

            ///////
            // removed buffer on Invoker, using overloadRatio
            ///////
            /*int remainingBufferCapacity = invokerBufferSize - invoker.getBufferSize();
            newInvokerActivationsIterator = newInvokerActivations.iterator();
            while (remainingBufferCapacity-- > 0 && newInvokerActivationsIterator.hasNext()) {
                final IBufferizable activation = newInvokerActivationsIterator.next();
                invoker.buffering(activation);
                activationsScheduled.add(activation);
                newInvokerActivationsIterator.remove();
            }*/
        }

        return activationsScheduled;
    }

    private @Nonnull List<Invoker> processCompletions(@Nonnull final Collection<Completion> completions,
                                                      @Nonnull final Map<String, Invoker> invokersMap) {
        final Set<Invoker> invokersWithCompletions = new HashSet<>(invokersMap.size());
        if (completions.isEmpty()) return new ArrayList<>(invokersWithCompletions);

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

            // release resources associated with this completion (even if invoker is not healthy)
            invoker.release(activationId);

            // add invoker to the set of invokers that have processed at least one completion
            // note that, if n completions have been received, it is not sufficient check for
            //   first n buffered activations because it is possible that one of the completion processed
            //   released enough resources for m, with m >= n, buffered activations
            invokersWithCompletions.add(invoker);
        }

        return new ArrayList<>(invokersWithCompletions);
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
        final Map<String, Integer> activationsBufferTrace = buffer.stream()
                .collect(groupingBy(IBufferizable::getTargetInvoker, toUnmodifiableList()))
                .entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
        LOG.trace("Buffer - {}.", activationsBufferTrace);
    }

    private void bufferStats(@Nonnull final Map<String, Queue<IBufferizable>> invokerBufferMap) {
        LOG.trace("Buffer - {}.", invokerBufferMap.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> e.getValue().size())));
    }

    private void send(@Nonnull final Queue<? extends ISchedulable> schedulables) {
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

    private @Nonnull CompletionKafkaConsumer createCompletionConsumerFrom(@Nonnull String instance) {
        checkNotNull(instance, "Instance can not be null.");
        final String topic = String.format(TEMPLATE_COMPLETION_TOPIC, instance);
        LOG.trace("Creating new completion Kafka consumer for topic: {}.", topic);
        final CompletionKafkaConsumer completionKafkaConsumer = new CompletionKafkaConsumer(
                List.of(topic), kafkaConsumerProperties, 50
        );
        register(List.of(completionKafkaConsumer));
        completionKafkaConsumer.register(List.of(this));
        Objects.requireNonNull(schedulerExecutors.networkIO()).submit(completionKafkaConsumer);
        return completionKafkaConsumer;
    }

    private void schedulePeriodicActivities() {
        // release activations too old, assuming there was an error sending completion
        Objects.requireNonNull(schedulerPeriodicExecutors.computation()).scheduleWithFixedDelay(
                () -> releaseActivationsOlderThan(runningActivationTimeLimitMs),
                0L,
                runningActivationTimeLimitMs,
                TimeUnit.MILLISECONDS
        );
    }

    public static @Nonnull String getInvokerTargetFrom(@Nonnull InvokerInstance invokerInstance) {
        return invokerInstance.getInstanceType().getName() + invokerInstance.getInstance();
    }

    public static int getInstanceFrom(@Nonnull String invokerTarget) {
        return Integer.parseInt(invokerTarget.replace("invoker", ""));
    }

    public static boolean isInvokerHealthTestAction(@Nullable Action action) {
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

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

}