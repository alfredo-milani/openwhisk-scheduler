package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InvokerTest {

    @Test
    public void tryAcquireFromBuffer() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String record1 = "{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856ba\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":1,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String record2 = "{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856bb\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":1,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String record3 = "{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856bc\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":1,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String record4 = "{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856bd\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":1,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String record5 = "{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856be\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":1,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final String record6 = "{\"action\":{\"name\":\"fn2\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"93a8dd0750934856a8dd0750935856bf\",\"blocking\":true,\"cause\":\"7402792454a04aca82792454a09aca14\",\"content\":{\"$scheduler\":{\"duration\":10,\"limits\":{\"concurrency\":1,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"extras\":{\"cmd\":\"Sleep executed for 5 s.\",\"fn_name\":\"/guest/fn1\"},\"message\":\"Hello Kira!\",\"sleep_time\":5},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-f5098d6fdb4e59dd7348545470ab110f\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"WSfYUfdxl3hm7HHsmykAp1nrFwGKGC8l\",1613391003698,[\"MDEXs94DFVXmCKZmRgdIFSA7Zcsp2x0j\",1613391003260,[\"R9O0tKt4PnXSUz9Qu97g6thHCSP6jgrw\",1613390993698,[\"fvxblQafi4egIwb6Hw4KWokFLCbZh305\",1613390988735,[\"P8XxEP8sFUs5g9Gf8pykM4SjpGp6AiQA\",1613390988230]]]]],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        final Activation activation1 = objectMapper.readValue(record1, Activation.class);
        final Activation activation2 = objectMapper.readValue(record2, Activation.class);
        final Activation activation3 = objectMapper.readValue(record3, Activation.class);
        final Activation activation4 = objectMapper.readValue(record4, Activation.class);
        final Activation activation5 = objectMapper.readValue(record5, Activation.class);
        final Activation activation6 = objectMapper.readValue(record6, Activation.class);

        final Invoker invoker = new Invoker("invoker0", 256 * 3, 2);
        assertFalse(invoker.tryAcquireFromBuffer());

        assertTrue(invoker.tryAcquireMemoryAndConcurrency(activation1));
        assertTrue(invoker.tryAcquireMemoryAndConcurrency(activation2));
        assertTrue(invoker.tryAcquireMemoryAndConcurrency(activation3));
        assertFalse(invoker.tryAcquireMemoryAndConcurrency(activation4));

        assertTrue(invoker.tryBuffering(activation4));
        assertTrue(invoker.tryBuffering(activation5));
        assertFalse(invoker.tryBuffering(activation6));

        invoker.release(activation6.getActivationId());
        assertFalse(invoker.tryAcquireFromBuffer());
        assertFalse(invoker.tryAcquireMemoryAndConcurrency(activation6));
        assertFalse(invoker.tryBuffering(activation6));

        invoker.release(activation4.getActivationId());
        assertFalse(invoker.tryAcquireFromBuffer());
        assertFalse(invoker.tryAcquireMemoryAndConcurrency(activation6));
        assertTrue(invoker.tryBuffering(activation6));

        invoker.release(activation3.getActivationId());
        assertFalse(invoker.tryBuffering(activation6));
        invoker.tryAcquireFromBuffer();
        assertFalse(invoker.tryAcquireMemoryAndConcurrency(activation6));
        assertTrue(invoker.tryBuffering(activation6));
    }

}