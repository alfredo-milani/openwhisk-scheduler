package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.AbstractKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.HealthKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Health;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class HealthKafkaConsumerMock extends AbstractKafkaConsumer<Health> {

    private final static Logger LOG = LogManager.getLogger(HealthKafkaConsumer.class.getCanonicalName());

    public final static int DEFAULT_POLLING_INTERVAL_MS = 500;

    protected final ObjectMapper objectMapper;
    protected final int pollingIntervalMs;

    public HealthKafkaConsumerMock(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties) {
        this(topics, kafkaProperties, DEFAULT_POLLING_INTERVAL_MS);
    }

    public HealthKafkaConsumerMock(@Nonnull List<String> topics, @Nonnull Properties kafkaProperties, int pollingIntervalMs) {
        super(topics, kafkaProperties);
        checkArgument(pollingIntervalMs > 0, "Polling interval must be > 0.");
        this.pollingIntervalMs = pollingIntervalMs;
        this.objectMapper = new ObjectMapper();
    }

    public static final UUID HEALTH_STREAM = UUID.randomUUID();

    @Override
    public @Nonnull <T> Map<UUID, Collection<T>> streamsPartition(@Nonnull final Collection<T> data) {
        return new HashMap<>() {{
            put(HEALTH_STREAM, data);
        }};
    }

    private final Random random = new Random();
    private final String recordHealth = "{\"name\":{\"instance\":%d,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-%d\",\"userMemory\":\"%d B\"}}";

    /**
     * It is assumed that only one thread per instance calls this method.
     * @return
     */
    @Override
    public @Nullable Collection<Health> consume() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final Collection<Health> data = new ArrayDeque<>(10);
        for (int i = 0; i < 10; ++i) {
            try {
                int invoker = random.ints(0, 3).findFirst().getAsInt();
                Health activation = objectMapper.readValue(String.format(
                        recordHealth,
                        invoker,
                        invoker,
                        1024L * 1024L * 1024L * 2L
                ), Health.class);
                data.add(activation);
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: ");
                e.printStackTrace();
            }
        }
        LOG.trace("Sending {} consumable to observers.", data.size());
        return data;
    }

}