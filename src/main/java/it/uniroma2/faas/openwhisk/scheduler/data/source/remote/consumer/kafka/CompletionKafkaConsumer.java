package it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.BlockingCompletion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.FailureCompletion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.NonBlockingCompletion;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class CompletionKafkaConsumer extends AbstractKafkaConsumer<Completion> {

    private final static Logger LOG = LogManager.getLogger(CompletionKafkaConsumer.class.getCanonicalName());

    public final static int DEFAULT_POLLING_INTERVAL_MS = 500;

    protected final ObjectMapper objectMapper;
    protected final int pollingIntervalMs;

    public CompletionKafkaConsumer(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties) {
        this(topics, kafkaProperties, DEFAULT_POLLING_INTERVAL_MS);
    }

    public CompletionKafkaConsumer(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties, int pollingIntervalMs) {
        super(topics, kafkaProperties);
        checkArgument(pollingIntervalMs > 0, "Polling interval must be > 0.");
        this.pollingIntervalMs = pollingIntervalMs;
        this.objectMapper = new ObjectMapper();
    }

    public static final UUID COMPLETION_STREAM = UUID.randomUUID();

    @Override
    public @Nonnull <T> Map<UUID, Collection<T>> streamsPartition(@Nonnull final Collection<T> data) {
        return new HashMap<>() {{
            put(COMPLETION_STREAM, data);
        }};
    }

    /**
     * It is assumed that only one thread per instance calls this method.
     * @return
     */
    @Override
    public @Nullable Collection<Completion> consume() {
        // see@ https://stackoverflow.com/questions/58697750/fetch-max-wait-ms-vs-parameter-to-poll-method
        //  or in docs for poll() details
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollingIntervalMs));
        if (records.count() == 0) return null;

        LOG.trace("Read {} records from topics {}.", records.count(), topics);
        final Collection<Completion> data = new ArrayDeque<>(records.count());
        for (ConsumerRecord<String, String> r : records) {
            try {
                data.add(objectMapper.readValue(r.value(), NonBlockingCompletion.class));
            } catch (JsonProcessingException e0) {
                try {
                    data.add(objectMapper.readValue(r.value(), BlockingCompletion.class));
                } catch (JsonProcessingException e1) {
                    try {
                        data.add(objectMapper.readValue(r.value(), FailureCompletion.class));
                    } catch (JsonProcessingException e2) {
                        LOG.warn("Exception parsing Activation from record: {}.", r.value());
                    }
                }
            }
        }
        LOG.trace("Sending {} consumable to observers.", data.size());
        return data;
    }

}