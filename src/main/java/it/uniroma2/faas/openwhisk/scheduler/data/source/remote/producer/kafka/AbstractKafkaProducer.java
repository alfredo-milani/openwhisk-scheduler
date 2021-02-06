package it.uniroma2.faas.openwhisk.scheduler.data.source.remote.producer.kafka;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IProducer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public abstract class AbstractKafkaProducer implements IProducer {

    public final static int THREAD_COUNT = 2;

    protected final Properties kafkaProperties;
    protected final ExecutorService executors;
    protected final KafkaProducer<String, String> producer;

    public AbstractKafkaProducer(Properties kafkaProperties) {
        this(kafkaProperties, null);
    }

    public AbstractKafkaProducer(Properties kafkaProperties, ExecutorService executors) {
        this.kafkaProperties = kafkaProperties;
        this.executors = executors;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.kafkaProperties);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

}