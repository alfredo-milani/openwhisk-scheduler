package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.AbstractKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.CompletionKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CompletionKafkaConsumerMock extends AbstractKafkaConsumer<Completion> {

    private final static Logger LOG = LogManager.getLogger(CompletionKafkaConsumer.class.getCanonicalName());

    public final static int DEFAULT_POLLING_INTERVAL_MS = 500;

    protected final ObjectMapper objectMapper;
    protected final int pollingIntervalMs;

    public CompletionKafkaConsumerMock(@Nonnull List<String> topics, Properties kafkaProperties) {
        this(topics, kafkaProperties, DEFAULT_POLLING_INTERVAL_MS);
    }

    public CompletionKafkaConsumerMock(@Nonnull List<String> topics, Properties kafkaProperties, int pollingIntervalMs) {
        super(topics, kafkaProperties);
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

    private final String completionBlockingResult = "{\"instance\":{\"instance\":0,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-0\",\"userMemory\":\"2147483648 B\"},\"isSystemError\":false,\"response\":{\"activationId\":\"50bd97db9fef433abd97db9fef933ab1\",\"annotations\":[{\"key\":\"path\",\"value\":\"guest/sleep_one\"},{\"key\":\"waitTime\",\"value\":4545},{\"key\":\"kind\",\"value\":\"python:3\"},{\"key\":\"timeout\",\"value\":false},{\"key\":\"limits\",\"value\":{\"concurrency\":5,\"logs\":10,\"memory\":256,\"timeout\":60000}},{\"key\":\"initTime\",\"value\":28}],\"duration\":1037,\"end\":1612889796543,\"logs\":[],\"name\":\"sleep_one\",\"namespace\":\"guest\",\"publish\":false,\"response\":{\"result\":{\"sleep_one\":{\"$scheduler\":{\"limits\":{\"concurrency\":5,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"target\":\"invoker0\"},\"scheduler\":{\"priority\":0,\"target\":\"invoker0\"},\"sleep\":1}},\"size\":199,\"statusCode\":0},\"start\":1612889795506,\"subject\":\"guest\",\"version\":\"0.0.1\"},\"transid\":[\"iud25hHHjA0mtOTri9nKePhEnDhcMpH2\",1612889790960]}";
    private final String completionNonBlocking = "{\"activationId\":\"811bd520fa794dbe9bd520fa793dbef3\",\"instance\":{\"instance\":0,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-0\",\"userMemory\":\"2147483648 B\"},\"isSystemError\":false,\"transid\":[\"MSCztINkcSGlhgeQT6H7YJAPaDVNO0nK\",1612935312054]}";

    /**
     * It is assumed that only one thread per instance calls this method.
     * @return
     */
    @Override
    public @Nullable Collection<Completion> consume() {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final Collection<Completion> data = new ArrayDeque<>(10);
        for (int i = 0; i < 5; ++i) {
            try {
                Completion completion = objectMapper.readValue(String.format(completionNonBlocking), Completion.class);
                data.add(completion);
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: ");
                e.printStackTrace();
            }
        }
        for (int i = 0; i < 5; ++i) {
            try {
                Completion completion = objectMapper.readValue(String.format(completionBlockingResult), Completion.class);
                data.add(completion);
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: ");
                e.printStackTrace();
            }
        }

        LOG.trace("Sending {} consumable to observers.", data.size());
        return data;
    }

}