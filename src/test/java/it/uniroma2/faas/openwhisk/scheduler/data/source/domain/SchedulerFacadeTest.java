package it.uniroma2.faas.openwhisk.scheduler.data.source.domain;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IObserver;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.ActivationKafkaConsumerMock;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.ActivationKafkaProducerMock;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.BaseSchedulerMock;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.TracerSchedulerMock;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.producer.kafka.AbstractKafkaProducer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced.BufferedScheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.Policy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.PolicyFactory;
import it.uniroma2.faas.openwhisk.scheduler.util.AppExecutors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.System.exit;

public class SchedulerFacadeTest {

    private final static Logger LOG = LogManager.getRootLogger();

    public static final String SOURCE_TOPIC = "scheduler";

    public static void start() {
        Configurator.setAllLevels(LOG.getName(), Level.TRACE);
        Configurator.setRootLevel(Level.TRACE);

        // create global app executors
        AppExecutors executors = new AppExecutors(
                Executors.newFixedThreadPool(2),
                null
        );

        // see@ https://stackoverflow.com/questions/51753883/increase-the-number-of-messages-read-by-a-kafka-consumer-in-a-single-poll
        // see@ https://stackoverflow.com/questions/61552431/kafka-consumer-poll-multiple-records-fetch
        // kafka consumer
        Properties kafkaConsumerProperties = new Properties() {{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            put(ConsumerConfig.GROUP_ID_CONFIG, "ow-scheduler-consumer");
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1_000);
            // end session after 30 s
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
            // wait for 5 MiB of data
            put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 500);
            // if min bytes has not reached limit, wait for 2000 ms
            put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
            // min bytes to fetch for each partition from broker server
            put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1);
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }};

        final ActivationKafkaConsumerMock<Activation> activationKafkaConsumerMock = new ActivationKafkaConsumerMock<>(
                List.of(SOURCE_TOPIC), kafkaConsumerProperties, 500
        );
//        BaseKafkaConsumer healthKafkaConsumer = new HealthKafkaConsumer(
//                List.of("health"), kafkaConsumerProperties, null);

        // kafka producer
        Properties kafkaProducerProperties = new Properties() {{
            put(ProducerConfig.CLIENT_ID_CONFIG, AbstractKafkaProducer.class.getSimpleName());
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            // wait acks only from leader
            put(ProducerConfig.ACKS_CONFIG, "1");  // 1 -> leader
            put(ProducerConfig.RETRIES_CONFIG, 0);
//            put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
            put(ProducerConfig.LINGER_MS_CONFIG, 10);
//            put(ProducerConfig.BUFFER_MEMORY_CONFIG, 100 * 1024);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }};
        final AbstractKafkaProducer activationKafkaProducer = new ActivationKafkaProducerMock(kafkaProducerProperties, null);

        // define scheduler
        IPolicy policy = PolicyFactory.createPolicy(Policy.PRIORITY_QUEUE_FIFO);
        LOG.trace("Scheduler policy selected: {}.", policy.getPolicy());
        Scheduler scheduler = new BaseSchedulerMock(policy, activationKafkaProducer);
        LOG.trace("Creating Scheduler {}.", scheduler.getClass().getSimpleName());
        boolean tracerSchedulerOption = true;
        boolean bufferedSchedulerOption = false;
        if (tracerSchedulerOption) {
            scheduler = new TracerSchedulerMock(scheduler);
            ((TracerSchedulerMock) scheduler).setTimeLimitMs(TimeUnit.SECONDS.toMillis(5));
            LOG.trace("Adding Scheduler {}.", scheduler.getClass().getSimpleName());
        }
        if (bufferedSchedulerOption) {
            BufferedScheduler bufferedScheduler = new BufferedScheduler(scheduler);
            bufferedScheduler.setThreshold(75.0f);
            scheduler = bufferedScheduler;
            LOG.trace("Adding Scheduler {}.", scheduler.getClass().getSimpleName());
        }

        List<IObserver> activationObservers = new ArrayList<>();
        activationObservers.add(scheduler);
//        List<IObserver> healthObservers = new ArrayList<>() {{
//            add(scheduler);
//        }};
        activationKafkaConsumerMock.register(activationObservers);
//        healthKafkaConsumer.register(healthObservers);

        // data source consumer list
        List<Callable<String>> consumers = new ArrayList<>() {{
            add(activationKafkaConsumerMock);
//            add(healthKafkaConsumer);
        }};

        // register hook to release resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            activationKafkaConsumerMock.close();
//            healthKafkaConsumer.close();
            activationKafkaProducer.close();
            executors.shutdown();
            LogManager.shutdown();
        }));

        try {
            // see@ https://stackoverflow.com/questions/20495414/thread-join-equivalent-in-executor
            // invokeAll() blocks until all tasks are completed
            executors.networkIO().invokeAll(consumers);
        } catch (InterruptedException e) {
            LOG.fatal("Scheduler interrupted: {}.", e.getMessage());
            exit(1);
        }

    }

    public static void main(String[] args) {
        start();
    }

}