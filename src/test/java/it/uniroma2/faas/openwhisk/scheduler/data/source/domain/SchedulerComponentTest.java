package it.uniroma2.faas.openwhisk.scheduler.data.source.domain;

import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock.*;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.producer.kafka.AbstractKafkaProducer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.Policy;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.PolicyFactory;
import it.uniroma2.faas.openwhisk.scheduler.util.SchedulerExecutors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import static java.lang.System.exit;

public class SchedulerComponentTest {

    private final static Logger LOG = LogManager.getRootLogger();

    public static final String SCHEDULER_TOPIC = "scheduler";
    public static final String HEALTH_TOPIC = "health";
    public static final String COMPLETION_TOPIC = "completed";
    public static final String EVENT_TOPIC = "events";

    public static void start() {
        Configurator.setAllLevels(LOG.getName(), Level.TRACE);
        Configurator.setRootLevel(Level.TRACE);

        // create global app executors
        SchedulerExecutors executors = new SchedulerExecutors(3, 0);

        // entities
        List<Callable<String>> dataSourceConsumers = new ArrayList<>();
        List<Closeable> closeables = new ArrayList<>();

        // see@ https://stackoverflow.com/questions/51753883/increase-the-number-of-messages-read-by-a-kafka-consumer-in-a-single-poll
        // see@ https://stackoverflow.com/questions/61552431/kafka-consumer-poll-multiple-records-fetch
        // kafka consumer
        Properties kafkaConsumerProperties = new Properties() {{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            put(ConsumerConfig.GROUP_ID_CONFIG, "ow-scheduler-consumer");
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1_000);
            // end session after 30 s
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15_000);
            // wait for 5 MiB of data
            put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
            // if min bytes has not reached limit, wait for 2000 ms
            put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
            // min bytes to fetch for each partition from broker server
            put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 0);
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }};

        final ActivationKafkaConsumerMock activationsKafkaConsumer = new ActivationKafkaConsumerMock(
                List.of(SCHEDULER_TOPIC), kafkaConsumerProperties, 50
        );

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
        final ActivationKafkaProducerMock activationsKafkaProducer = new ActivationKafkaProducerMock(kafkaProducerProperties, null);

        // define scheduler
        // final IPolicy policy = PolicyFactory.createPolicy(Policy.PASS_THROUGH);
        // final IPolicy policy = PolicyFactory.createPolicy(Policy.PRIORITY_QUEUE_FIFO);
        final IPolicy policy = PolicyFactory.createPolicy(Policy.SHORTEST_JOB_FIRST);
        LOG.trace("Scheduler policy selected: {}.", policy.getPolicy());
        Scheduler scheduler;
        boolean tracerSchedulerOption = false;
        boolean bufferedSchedulerOption = true;
        boolean healthScheckerSchedulerOption = false;
        if (bufferedSchedulerOption) {
            scheduler = new BufferedSchedulerMock(policy, activationsKafkaProducer);
            ((BufferedSchedulerMock) scheduler).setKafkaBootstrapServers("localhost:9092");
            ((BufferedSchedulerMock) scheduler).setMaxBufferSize(500);
            LOG.trace("Enabled scheduler functionality - {}.", scheduler.getClass().getSimpleName());
            /*final HealthKafkaConsumerMock healthKafkaConsumer = new HealthKafkaConsumerMock(
                    List.of(HEALTH_TOPIC), kafkaConsumerProperties, 500
            );
            healthKafkaConsumer.register(List.of(scheduler));
            dataSourceConsumers.add(healthKafkaConsumer);
            closeables.add(healthKafkaConsumer);*/
        } else {
            scheduler = new BaseSchedulerMock(policy, activationsKafkaProducer);
        }
        LOG.trace("Creating Scheduler {}.", scheduler.getClass().getSimpleName());

        if (tracerSchedulerOption) {
            scheduler = new TracerSchedulerMock(scheduler);
            LOG.trace("Enabled scheduler functionality - {}.", scheduler.getClass().getSimpleName());
            final EventKafkaConsumerMock eventKafkaConsumerMock = new EventKafkaConsumerMock(
                    List.of(EVENT_TOPIC), kafkaConsumerProperties, 500
            );
            eventKafkaConsumerMock.register(List.of(scheduler));
            dataSourceConsumers.add(eventKafkaConsumerMock);
            closeables.add(eventKafkaConsumerMock);
        }
        if (healthScheckerSchedulerOption) {
            scheduler = new HealthCheckerSchedulerMock(scheduler);
            LOG.trace("Enabled scheduler functionlity - {}.", scheduler.getClass().getSimpleName());
            final HealthKafkaConsumerMock healthKafkaConsumer = new HealthKafkaConsumerMock(
                    List.of(HEALTH_TOPIC), kafkaConsumerProperties, 500
            );
            healthKafkaConsumer.register(List.of(scheduler));
            dataSourceConsumers.add(healthKafkaConsumer);
            closeables.add(healthKafkaConsumer);

            final CompletionKafkaConsumerMock completionKafkaConsumer = new CompletionKafkaConsumerMock(
                    List.of(HEALTH_TOPIC), kafkaConsumerProperties, 500
            );
            completionKafkaConsumer.register(List.of(scheduler));
            dataSourceConsumers.add(completionKafkaConsumer);
            closeables.add(completionKafkaConsumer);
        }

        activationsKafkaConsumer.register(List.of(scheduler));
        dataSourceConsumers.add(activationsKafkaConsumer);
        closeables.addAll(List.of(activationsKafkaConsumer, activationsKafkaProducer));

        // register hook to release resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            closeables.forEach(closeable -> {
                try {
                    closeable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            executors.shutdown();
            LogManager.shutdown();
        }));

        try {
            // see@ https://stackoverflow.com/questions/20495414/thread-join-equivalent-in-executor
            // invokeAll() blocks until all tasks are completed
            executors.networkIO().invokeAll(dataSourceConsumers);
        } catch (InterruptedException e) {
            LOG.fatal("Scheduler interrupted: {}.", e.getMessage());
            exit(1);
        }

    }

    public static void main(String[] args) {
        start();
    }

}