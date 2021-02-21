package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.AbstractKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.data.source.remote.consumer.kafka.ConsumableKafkaConsumer;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.util.LineReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
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

    private final Queue<String> activationCompositionQueue = new ArrayDeque<>(10) {{
       add("{\"action\":{\"name\":\"cmp\",\"path\":\"guest\",\"version\":\"0.0.2\"},\"activationId\":\"dafbf923d166490dbbf923d166190d17\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"limits\":{\"concurrency\":3,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":5,\"target\":\"invoker0\",\"duration\":128},\"sleep_time\":5,\"user\":\"Kira\"},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"2-46afd33766367b04df0fefde394c5027\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}");
       add("{\"action\":{\"name\":\"fn1\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"2536b89d12c44a16b6b89d12c48a161c\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":128,\"limits\":{\"concurrency\":3,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":5,\"target\":\"invoker0\"},\"sleep_time\":5,\"user\":\"Kira\"},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-982cb15dce458913114cb2f6a1e58e4d\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}");
       add("{\"action\":{\"name\":\"cmp\",\"path\":\"guest\",\"version\":\"0.0.2\"},\"activationId\":\"1782c4fb97624eea82c4fb97620eea50\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"limits\":{\"concurrency\":3,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"target\":\"invoker0\",\"priority\":0,\"duration\":10},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"$composer\":{\"resuming\":true,\"session\":\"dafbf923d166490dbbf923d166190d17\",\"stack\":[],\"state\":2},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"2-46afd33766367b04df0fefde394c5027\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}");
       add("{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856ba\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":3,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}");
       add("{\"action\":{\"name\":\"cmp\",\"path\":\"guest\",\"version\":\"0.0.2\"},\"activationId\":\"094700d725eb40fd8700d725eba0fd5e\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"limits\":{\"concurrency\":3,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"target\":\"invoker0\",\"priority\":0,\"duration\":14},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn2\"},\"$composer\":{\"resuming\":true,\"session\":\"dafbf923d166490dbbf923d166190d17\",\"stack\":[],\"state\":3},\"message\":\"Hello guest!\"},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"2-46afd33766367b04df0fefde394c5027\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"zFH9e2zMUBKX04lGjmwXz2chyN0fSChW\",1613391012468,[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}");
    }};

    private final String traceFile = "/Volumes/Data/Projects/FaaS/OpenWhisk/openwhisk-scheduler/src/test/res/tracer_scheduler/invoker0.txt";
    private final String activationsComposition = "/Volumes/Data/Projects/FaaS/OpenWhisk/openwhisk-scheduler/src/test/res/tracer_scheduler/c0_composition.txt";
    private LineReader lineReader;
    {
        try {
            lineReader = new LineReader(activationsComposition);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

        /*try {
            data.add(objectMapper.readValue(lineReader.poll(), Activation.class));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }*/

        /*for (int i = 0; i < 5; ++i) {
            final String record = lineReader.poll();
            try {
                data.add(objectMapper.readValue(record, Activation.class));
            } catch (JsonProcessingException e) {
                LOG.warn("Exception parsing Activation from record: {}.", record);
            }
        }*/

        try {
            if (!activationCompositionQueue.isEmpty())
                data.add(objectMapper.readValue(activationCompositionQueue.poll(), Activation.class));
        } catch (JsonProcessingException e) {
            LOG.warn("Exception parsing Activation from record.");
            e.printStackTrace();
        }

        /*for (int i = 0; i < 10; ++i) {
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