package it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.IConsumable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;

public class ConsumableKafkaConsumer<T extends IConsumable> extends AbstractKafkaConsumer<T> {

    private final static Logger LOG = LogManager.getLogger(ConsumableKafkaConsumer.class.getCanonicalName());

    public final static int DEFAULT_POLLING_INTERVAL_MS = 500;

    // see@ https://stackoverflow.com/questions/6846244/jackson-and-generic-type-reference
    // see@ https://stackoverflow.com/questions/26066867/class-of-generic-class
    protected T targetClass;
    protected final JavaType classType;
    protected final ObjectMapper objectMapper;
    protected final int pollingIntervalMs;

    public ConsumableKafkaConsumer(@Nonnull List<String> topics, Properties kafkaProperties) {
        this(topics, kafkaProperties, DEFAULT_POLLING_INTERVAL_MS);
    }

    public ConsumableKafkaConsumer(@Nonnull List<String> topics, Properties kafkaProperties, int pollingIntervalMs) {
        super(topics, kafkaProperties);
        this.pollingIntervalMs = pollingIntervalMs;
        this.objectMapper = new ObjectMapper();
        // NOTE: to resolve type parameter for jackson deserialization, class type must be resolved.
        //  In order to work, class type of required type parameter must be provided from caller.
        //  see@ https://stackoverflow.com/questions/11664894/jackson-deserialize-using-generic-class
        //  see@ https://stackoverflow.com/questions/6846244/jackson-and-generic-type-reference
//        this.classType = objectMapper.getTypeFactory().constructType(targetClass.getClass());
        this.classType = null;
    }

    // to stream type String
//    public static final String SUCCESS_STREAM = "success_stream";
//    public static final String FAILURE_STREAM = "failure_stream";
    // to stream type ISubject.IStream
//    public static final class SUCCESS_STREAM implements IStream {
//        public static final SUCCESS_STREAM INSTANCE = new SUCCESS_STREAM();
//        private SUCCESS_STREAM() {}
//    }
//    public static final class FAILURE_STREAM implements IStream {
//        public static final FAILURE_STREAM INSTANCE = new FAILURE_STREAM();
//        private FAILURE_STREAM() {}
//    }
    // to stream type UUID
    public static final UUID SUCCESS_STREAM = UUID.randomUUID();
    public static final UUID FAILURE_STREAM = UUID.randomUUID();

    @Override
    public @Nonnull <S> Map<UUID, Collection<S>> streamsPartition(@Nonnull Collection<S> data) {
        List<S> coll = new ArrayList<>(data);
        Collection<S> firstHalf = new ArrayList<>();
        Collection<S> secondHalf = new ArrayList<>();
        for (int i = 0; i < coll.size() / 2; ++i) {
            firstHalf.add(coll.get(i));
        }
        for (int i = coll.size() / 2; i < coll.size() - 1; ++i) {
            secondHalf.add(coll.get(i));
        }
        Map<UUID, Collection<S>> map = new HashMap<>() {{
            put(SUCCESS_STREAM, firstHalf);
            put(FAILURE_STREAM, secondHalf);
        }};

        return map;
    }

    /**
     * It is assumed that only one thread per instance calls this method.
     * @return
     */
    @Override
    public @Nullable Collection<T> consume() {
        // see@ https://stackoverflow.com/questions/58697750/fetch-max-wait-ms-vs-parameter-to-poll-method
        //  or in docs for poll() details
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollingIntervalMs));
        if (records.count() == 0) return null;

        LOG.trace("Read {} records from topics {}.", records.count(), topics);
        final Collection<T> data = new ArrayDeque<>(records.count());
        for (ConsumerRecord<String, String> r : records) {
            try {
                data.add(objectMapper.readValue(r.value(), classType));
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: {}.", r.value());
            }
        }
        LOG.trace("Sending {} consumable to observers.", data.size());
        return data;
    }

}