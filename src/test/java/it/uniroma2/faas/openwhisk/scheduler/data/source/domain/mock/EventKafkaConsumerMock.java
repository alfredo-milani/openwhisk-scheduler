package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.AbstractKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Event;
import it.uniroma2.faas.openwhisk.scheduler.util.LineReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class EventKafkaConsumerMock extends AbstractKafkaConsumer<Event> {

    private final static Logger LOG = LogManager.getLogger(it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.EventKafkaConsumer.class.getCanonicalName());

    public final static int DEFAULT_POLLING_INTERVAL_MS = 500;

    protected final ObjectMapper objectMapper;
    protected final int pollingIntervalMs;

    public EventKafkaConsumerMock(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties) {
        this(topics, kafkaProperties, DEFAULT_POLLING_INTERVAL_MS);
    }

    public EventKafkaConsumerMock(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties,
                                  int pollingIntervalMs) {
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

    private LineReader lineReader;
    {
        try {
            lineReader = new LineReader("/Volumes/Data/Projects/FaaS/OpenWhisk/openwhisk-scheduler/src/test/res/tracer_scheduler/events.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * It is assumed that only one thread per instance calls this method.
     * @return
     */
    @Override
    public @Nullable Collection<Event> consume() {
        try {
            TimeUnit.SECONDS.sleep(400);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final Collection<Event> data = new ArrayDeque<>(10);

        for (int i = 0; i < 10; ++i) {
            final String record = lineReader.poll();
            try {
                data.add(objectMapper.readValue(record, Event.class));
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: {}.", record);
            }
        }

        LOG.trace("Sending {} consumable to observers.", data.size());
        return data;
    }

}