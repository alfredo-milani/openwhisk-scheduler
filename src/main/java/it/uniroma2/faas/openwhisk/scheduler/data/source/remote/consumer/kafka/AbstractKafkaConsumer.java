package it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IConsumer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.IObserver;
import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract class which reads from Kafka data source and parse records to a specified type.
 * Every subclass should read from one or more Kafka topics and produce a specific type of {@link IConsumable}.
 *
 * @param <T> type of {@link IConsumable} selected.
 */
public abstract class AbstractKafkaConsumer<T extends IConsumable> implements IConsumer<T>, ISubject {

    private final static Logger LOG = LogManager.getLogger(AbstractKafkaConsumer.class.getCanonicalName());

    public final static int THREAD_COUNT = 5;

    protected final Object mutex = new Object();
    // OPTIMIZE: sostituisci con Set() per non avere duplicati in modo automatico
    protected final List<IObserver> observers = new ArrayList<>();
    protected Instant lastRecordTimestamp = Instant.now();
    protected Instant newRecordTimestamp = Instant.from(lastRecordTimestamp);

    protected final List<String> topics;
    protected final Properties kafkaProperties;
    protected final KafkaConsumer<String, String> consumer;

    public AbstractKafkaConsumer(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties) {
        checkNotNull(topics, "Topic lists can not be null.");
        checkNotNull(kafkaProperties, "Kafka properties can not be null.");
        this.topics = topics;
        this.kafkaProperties = kafkaProperties;
        this.consumer = new KafkaConsumer<>(this.kafkaProperties);
    }

    @Override
    public void register(@Nonnull List<IObserver> observers) {
        checkNotNull(observers, "Observers list can not be null.");
        synchronized (mutex) {
            observers.forEach(o -> {
                if (!this.observers.contains(o)) {
                    this.observers.add(o);
                }
            });
        }

        List<String> observersNames = new ArrayList<>(this.observers.size());
        this.observers.forEach(o -> observersNames.add(o.getClass().getSimpleName()));
        LOG.trace("Subject {} has registered observers. Current observers: {}.",
                this.getClass().getSimpleName(), observersNames);
    }

    @Override
    public void unregister(@Nonnull List<IObserver> observers) {
        checkNotNull(observers, "Observers list can not be null.");
        synchronized (mutex) {
            this.observers.removeAll(observers);
        }

        List<String> observersNames = new ArrayList<>(this.observers.size());
        this.observers.forEach(o -> observersNames.add(o.getClass().getSimpleName()));
        LOG.trace("Subject {} has unregistered observers. Current observers: {}.",
                this.getClass().getSimpleName(), observersNames);
    }

    @Override
    public <S> void notifyObservers(@Nonnull final Map<UUID, Collection<S>> streamToData) {
        List<IObserver> observers;
        synchronized (mutex) {
            if (!hasUpdates()) return;
            observers = new ArrayList<>(this.observers);
        }

        List<String> observersNames = new ArrayList<>(observers.size());
        observers.forEach(o -> observersNames.add(o.getClass().getSimpleName()));
        LOG.trace("{} notifying observers: [{}]", this.getClass().getSimpleName(),
                String.join(", ", observersNames));

        for (IObserver o : observers) {
            // using thread for each read interval may led activations
            //   to be processed unordered, breaking priority
            // executors.computation().execute(() -> o.newEvent(this, List.copyOf(data)));

            streamToData.keySet().stream()
                    .filter(Objects::nonNull)
                    .forEach(stream -> o.newEvent(stream,
                            Collections.unmodifiableCollection(streamToData.get(stream))));
        }
    }

    @Override
    public boolean hasUpdates() {
        return !lastRecordTimestamp.equals(newRecordTimestamp);
    }

    /**
     * Must be called when consumer has received new data from data source.
     * If this method will not be called then no observer will be notified.
     *
     * In the base implementation, update and hasUpdates do not account stream diversification.
     * This means that whenever the BaseKafkaConsumer receive new element, hasUpdates return true.
     */
    @Override
    public void update() {
        lastRecordTimestamp = newRecordTimestamp;
        newRecordTimestamp = Instant.now();

        // assure unique timestamp at granularity level of millis
        if (lastRecordTimestamp.equals(newRecordTimestamp)) {
            newRecordTimestamp = newRecordTimestamp.plusMillis(1);
        }
    }

    protected void prepare() throws Exception {
        // topics creation
        final Properties properties = new Properties() {{
           put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        }};
        final Admin admin = Admin.create(properties);
        final ListTopicsResult listedTopics = admin.listTopics();
        final Set<String> topicsSet = listedTopics.names().get();

        // OpenWhisk default - to ensure FIFO ordering within topics
        int partitions = 1;
        // OpenWhisk lets admin configure this parameter in deploy configuration
        // Here, let it hardcoded for simplicity
        short replicationFactor = 1;

        final Map<String, String> topicConfig = new HashMap<>() {{
            put(TopicConfig.SEGMENT_BYTES_CONFIG, "536870912");
            put(TopicConfig.RETENTION_MS_CONFIG, "172800000");
            put(TopicConfig.RETENTION_BYTES_CONFIG, "1073741824");
            // in OpenWhisk this parameter is configured dynamically
            put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1054644");
        }};
        for (final String topic : topics) {
            // topic already created by another component
            if (topicsSet.contains(topic)) continue;

            final NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor)
                    .configs(topicConfig);
            final CreateTopicsResult topicResult = admin.createTopics(Collections.singleton(newTopic));
            final KafkaFuture<Void> future = topicResult.values().get(topic);
            // blocks until topic creation or error
            future.get();
        }

        admin.close();

        // topics subscriptions
        consumer.subscribe(topics);
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private void process() throws Exception {
        while (true) {
            final Collection<T> data = consume();
            if (data == null || data.size() == 0) continue;

            update();
            notifyObservers(streamsPartition(data));
        }
    }

    protected void onWakeup() {
        // ignore for shutdown
    }

    protected void cleanup() {
        consumer.close();
    }

    @Override
    public @Nonnull final String call() throws Exception {
        prepare();

        try {
            process();
        } catch (WakeupException e) {
            onWakeup();
        } finally {
            cleanup();
        }

        return this.getClass().getSimpleName();
    }

    @Override
    public void close() {
        consumer.wakeup();
    }

}