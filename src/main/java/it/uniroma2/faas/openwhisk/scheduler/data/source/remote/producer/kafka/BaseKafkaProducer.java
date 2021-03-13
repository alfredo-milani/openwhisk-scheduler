package it.uniroma2.faas.openwhisk.scheduler.data.source.remote.producer.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class BaseKafkaProducer extends AbstractKafkaProducer {

    // TODO - prova a mettere lOG in classe base (anche nel consumer)
    private final static Logger LOG = LogManager.getLogger(BaseKafkaProducer.class.getCanonicalName());

    private final ObjectMapper objectMapper;

    public BaseKafkaProducer(Properties kafkaProperties) {
        super(kafkaProperties);
        this.objectMapper = new ObjectMapper();
    }

    public BaseKafkaProducer(Properties kafkaProperties, ExecutorService executors) {
        super(kafkaProperties, executors);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void produce(@Nonnull final String topic, @Nonnull final IConsumable datum) {
        try {
            // see@ openwhisk/common/scala/src/main/scala/org/apache/openwhisk/connector/kafka/KafkaProducerConnector.scala
            //  val record = new ProducerRecord[String, String](topic, "messages", msg.serialize)
            // OPTIMIZE: for now record key is hardcoded, in future can be created new interface method in
            //  IProducer to assign key dynamically
            producer.send(new ProducerRecord<>(topic, "messages", objectMapper.writeValueAsString(datum)));
        } catch (JsonProcessingException e) {
            LOG.warn("Exception parsing Activation from consumable: " + datum.toString());
        }
    }

    // OPTIMIZE: see@ http://cloudurable.com/blog/kafka-tutorial-kafka-producer-advanced-java-examples/index.html#:~:text=The%20Kafka%20Producer%20has%20a,acks%20to%20control%20record%20durability.
    @Override
    public void produce(@Nonnull final String topic, @Nonnull final Collection<? extends IConsumable> data) {
        try {
            data.forEach(c -> produce(topic, c));
        } catch (KafkaException e) {
            LOG.warn("Exception received while sending consumable to " + topic);
        }
    }

}