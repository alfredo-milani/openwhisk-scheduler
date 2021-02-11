package it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class EventKafkaConsumer extends AbstractKafkaConsumer<Event> {

    private final static Logger LOG = LogManager.getLogger(EventKafkaConsumer.class.getCanonicalName());

    public final static int DEFAULT_POLLING_INTERVAL_MS = 500;

    protected final ObjectMapper objectMapper;
    protected final int pollingIntervalMs;

    public EventKafkaConsumer(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties) {
        this(topics, kafkaProperties, DEFAULT_POLLING_INTERVAL_MS);
    }

    public EventKafkaConsumer(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties, int pollingIntervalMs) {
        super(topics, kafkaProperties);
        checkArgument(pollingIntervalMs > 0, "Polling interval must be > 0.");
        this.pollingIntervalMs = pollingIntervalMs;
        this.objectMapper = new ObjectMapper();
    }

    public static final UUID EVENT_STREAM = UUID.randomUUID();

    @Override
    public @Nonnull <T> Map<UUID, Collection<T>> streamsPartition(@Nonnull final Collection<T> data) {
        return new HashMap<>() {{
            put(EVENT_STREAM, data);
        }};
    }

    /**
     * It is assumed that only one thread per instance calls this method.
     * @return
     */
    @Override
    public @Nullable Collection<Event> consume() {
        // see@ https://stackoverflow.com/questions/58697750/fetch-max-wait-ms-vs-parameter-to-poll-method
        //  or in docs for poll() details
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollingIntervalMs));
        if (records.count() == 0) return null;

        LOG.trace("Read {} records from topics {}.", records.count(), topics);
        final Collection<Event> data = new ArrayDeque<>(records.count());
        for (ConsumerRecord<String, String> r : records) {
            try {
                data.add(objectMapper.readValue(r.value(), Event.class));
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: {}.", r.value());
            }
        }
        LOG.trace("Sending {} consumable to observers.", data.size());
        return data;
    }

}