package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.producer.kafka.AbstractKafkaProducer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.producer.kafka.BaseKafkaProducer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;

public class ActivationKafkaProducerMock extends AbstractKafkaProducer {

    private final static Logger LOG = LogManager.getLogger(BaseKafkaProducer.class.getCanonicalName());

    private final ObjectMapper objectMapper;

    public ActivationKafkaProducerMock(Properties kafkaProperties) {
        super(kafkaProperties);
        this.objectMapper = new ObjectMapper();
    }

    public ActivationKafkaProducerMock(Properties kafkaProperties, ExecutorService executors) {
        super(kafkaProperties, executors);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public <T extends IConsumable> void produce(@Nonnull final String topic, @Nonnull final T datum) {
        try {
            LOG.debug("Send record with key {} - {}", "messages", objectMapper.writeValueAsString(datum));
        } catch (JsonProcessingException e) {
            LOG.warn("Exception parsing Activation from consumable: " + datum.toString());
        }
    }

    @Override
    public <T extends IConsumable> void produce(@Nonnull final String topic, @Nonnull final Collection<T> data) {
        checkNotNull(topic, "Topic can not be null.");
        checkNotNull(data, "Data can not be null.");

        try {
            data.forEach(c -> produce(topic, c));
        } catch (KafkaException e) {
            LOG.warn("Exception received while sending consumable to " + topic);
        }
    }

}