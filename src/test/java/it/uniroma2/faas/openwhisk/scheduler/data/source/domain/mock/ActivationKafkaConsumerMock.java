package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.AbstractKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ConsumableKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ActivationKafkaConsumerMock extends AbstractKafkaConsumer<Activation> {

    private final static Logger LOG = LogManager.getLogger(ConsumableKafkaConsumer.class.getCanonicalName());

    protected final ObjectMapper objectMapper;
    protected final int pollingIntervalMs;

    public ActivationKafkaConsumerMock(@Nonnull List<String> topics, Properties kafkaProperties) {
        this(topics, kafkaProperties, 500);
    }

    public ActivationKafkaConsumerMock(@Nonnull List<String> topics, Properties kafkaProperties, int pollingIntervalMs) {
        super(topics, kafkaProperties);
        this.pollingIntervalMs = pollingIntervalMs;
        this.objectMapper = new ObjectMapper();
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
    public static final UUID ACTIVATION_STREAM = UUID.randomUUID();

    /*@Override
    public @Nonnull <S> Map<UUID, Collection<S>> streamsPartition(@Nonnull final Collection<S> data) {
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

        System.out.println(map.toString());

        return map;
    }*/

    @Override
    public @Nonnull <S> Map<UUID, Collection<S>> streamsPartition(@Nonnull final Collection<S> data) {
        return new HashMap<>(1) {{
            put(ACTIVATION_STREAM, data);
        }};
    }

    private final Random random = new Random();
    private final String recordOnlyTarget = "{\"action\":{\"name\":\"hello_py\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"%s\",\"blocking\":true,\"content\":{\"kTest\":\"vTest\",\"$scheduler\":{\"target\":\"invoker0\",\"priority\":%d,\"limits\":{\"concurrency\":8,\"memory\":128,\"timeout\":60000,\"userMemory\":2147483648}}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-ff07dcb3291545090f86e9fc1a01b5bf\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"4K6K9Uoz09O5ut42VEiFI9zrOAiJX8oq\",1611192819095],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
    private final String recordWithCause = "{\"action\":{\"name\":\"fn1\",\"path\":\"guest\",\"version\":\"0.0.2\"},\"activationId\":\"%s\",\"blocking\":true,\"cause\":\"%s\",\"content\":{\"sleep_time\":15,\"user\":\"Kira\",\"$scheduler\":{\"target\":\"invoker0\",\"priority\":%d,\"limits\":{\"concurrency\":8,\"memory\":128,\"timeout\":60000,\"userMemory\":2147483648}}},\"initArgs\":[],\"revision\":\"2-2bd48aaaf6e1721bca963e680eee313e\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"bxytKe9Qk3EYVEYQnpHs02NDFo5eKAd3\",1609870677855,[\"2rkHZR02ErZB6kol4tl5LN4oxiHB5VH8\",1609870676410,[\"iRFErOK5Hflz8qduy49vBKWWMFTG44IW\",1609870676273]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
    private final String activationBufferedScheduler = "{\"action\":{\"name\":\"hello_py\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"%s\",\"blocking\":true,\"content\":{\"kTest\":\"vTest\",\"$scheduler\":{\"target\":\"%s\",\"priority\":%d,\"limits\":{\"concurrency\":2,\"memory\":256,\"timeout\":60000,\"userMemory\":2147483648}}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-ff07dcb3291545090f86e9fc1a01b5bf\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"4K6K9Uoz09O5ut42VEiFI9zrOAiJX8oq\",1611192819095],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
    private final String actvHealth = "{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"6b11a248ee6f455b91a248ee6f755b16\",\"blocking\":false,\"content\":{\"$scheduler\":{\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1612979174367],\"user\":{\"authkey\":{\"api_key\":\"0e3cf605-baf0-49b1-bcf6-05baf0f9b163:Tk5Zk96bCXTWF9prxj6IOMTjpkugR3AygKNI1Q7TzyK4SNW3UzXYYQ0ldnO2tQZ4\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"%s\"},\"rights\":[],\"subject\":\"whisk.system\"}}";
    private final String actvRealAction = "{\"action\":{\"name\":\"sleep_one\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"%s\",\"blocking\":false,\"content\":{\"sleep\":55,\"$scheduler\":{\"limits\":{\"concurrency\":2,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"target\":\"invoker0\"}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-bde32fea158c3a7be58a7945f3d26f69\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"g8jKp8sBWl3aoT0MfaZ218eQHpFQHiZW\",1612935313392],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";

    private final Queue<String> activationQueue = new ArrayDeque<>(3) {{
        add("{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"746e9b27a7bb4382ae9b27a7bb6382ec\",\"blocking\":false,\"cause\":null,\"content\":{\"$scheduler\":{\"target\":\"invoker0\",\"priority\":0,\"duration\":124}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613291769810],\"user\":{\"authkey\":{\"api_key\":\"b2da888d-d4f9-44ca-9a88-8dd4f904cacc:2RPyAygN4DxZqbZntRv3UzyaeIGRJ1bh2tHpOuajnv9mNRxvaLnCRydYDvyxpCwv\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"b2da888d-d4f9-44ca-9a88-8dd4f904cacc\"},\"rights\":[],\"subject\":\"whisk.system\"}}");
        add("{\"action\":{\"name\":\"invokerHealthTestAction0\",\"path\":\"whisk.system\",\"version\":\"0.0.1\"},\"activationId\":\"766a4164fb8c4560aa4164fb8c556017\",\"blocking\":false,\"cause\":null,\"content\":{\"$scheduler\":{\"target\":\"invoker0\",\"priority\":0,\"duration\":14}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":null,\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"sid_invokerHealth\",1613291769810],\"user\":{\"authkey\":{\"api_key\":\"b2da888d-d4f9-44ca-9a88-8dd4f904cacc:2RPyAygN4DxZqbZntRv3UzyaeIGRJ1bh2tHpOuajnv9mNRxvaLnCRydYDvyxpCwv\"},\"limits\":{},\"namespace\":{\"name\":\"whisk.system\",\"uuid\":\"b2da888d-d4f9-44ca-9a88-8dd4f904cacc\"},\"rights\":[],\"subject\":\"whisk.system\"}}");
        add("{\"action\":{\"name\":\"sleep_one\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"eecaac7f2d2b4b428aac7f2d2bcb429f\",\"blocking\":false,\"cause\":null,\"content\":{\"$scheduler\":{\"limits\":{\"concurrency\":2,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"target\":\"invoker0\",\"priority\":0,\"duration\":43}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-32483c678adacdb4868a59c58e5c5de5\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"HCKGoiAtQEsBdp2C7mjWWOAdtWBr1HrS\",1613292060448],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}");
    }};

    /**
     * It is assumed that only one thread per instance calls this method.
     * @return
     */
    @Override
    public @Nullable Collection<Activation> consume() {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final Collection<Activation> data = new ArrayDeque<>(10);

        try {
            if (!activationQueue.isEmpty())
                data.add(objectMapper.readValue(activationQueue.poll(), Activation.class));
        } catch (JsonProcessingException e) {
            LOG.warn("Exception parsing Activation from record.");
            e.printStackTrace();
        }

        /*for (int i = 0; i < 15; ++i) {
            try {
                Activation activation = objectMapper.readValue(String.format(activationBufferedScheduler,
                        UUID.randomUUID(),
                        "invoker" + random.ints(0, 3).findFirst().getAsInt(),
                        random.ints(0, 6).findFirst().getAsInt()),
                        Activation.class);
                data.add(activation);
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: ");
                e.printStackTrace();
            }
        }*/

        /*try {
            Activation activation = objectMapper.readValue(String.format(actvHealth,
                    UUID.randomUUID()),
                    Activation.class);
            data.add(activation);
        } catch (JsonProcessingException e) {
            LOG.warn("Exception parsing Activation from record: ");
            e.printStackTrace();
        }*/

        LOG.trace("Sending {} consumable to observers.", data.size());
        return data;
    }

}