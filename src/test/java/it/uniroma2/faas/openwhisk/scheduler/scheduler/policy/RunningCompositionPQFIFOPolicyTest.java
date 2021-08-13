package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RunningCompositionPQFIFOPolicyTest {

    @Test
    public void whenReceivingSimpleActivation_PolicyAppliesCorrectly() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // activationId: e6cc478e63744eca8c478e63749ecaa3
        // cause:
        final String activationRecord = "{\"action\": {\"name\": \"sleep\", \"path\": \"guest\", \"version\": \"0.0.1\"}, " +
                "\"activationId\": \"%s\", \"blocking\": true, \"content\": " +
                "{\"$scheduler\": {\"kind\": \"python:3\", \"limits\": {\"concurrency\": 1, \"memory\": 256, " +
                "\"timeout\": 60000, \"userMemory\": 2048}, \"overload\": false, \"priority\": %d, " +
                "\"target\": \"invoker0\"}, \"sleep\": 1}, \"initArgs\": [], \"lockedArgs\": {}, " +
                "\"revision\": \"1-66eb6f53dff7a98cd55d9edf10da9e72\", \"rootControllerIndex\": " +
                "{\"asString\": \"0\", \"instanceType\": \"controller\"}, \"transid\": " +
                "[\"RZ8mLYoGBiCrVvBcA7EwU2bwjv7Sgywq\", 1615849424148], \"user\": {\"authkey\": " +
                "{\"api_key\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";

        final Activation activation1 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), 1), Activation.class);
        final Activation activation2 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), 2), Activation.class);
        final Activation activation3 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), 3), Activation.class);
        final Collection<Activation> activations = new ArrayList<>() {{
            add(activation2);
            add(activation1);
            add(activation3);
        }};

        final int compositionLimit = 2;
        final IPolicy policy = PolicyFactory.createPolicy(Policy.RUNNING_COMPOSITION_PQFIFO);
        ((RunningCompositionPQFIFOPolicy) policy).setRunningCompositionsLimit(compositionLimit);
        final Queue<? extends ISchedulable> invocationQueue = policy.apply(activations);

        assertEquals(0, invocationQueue.size(), "Invocation queue size must be 2");
    }

    @Test
    public void whenReceivingCompositionActivation_PolicyAppliesCorrectly() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // activationId: d558b9d3150a467298b9d3150a067276
        // cause: 2dd491e759e747919491e759e7b791e9
        final String activationRecord = "{\"action\": {\"name\": \"img_man\", \"path\": \"guest\", \"version\": \"0.0.2\"}, " +
                "\"activationId\": \"%s\", \"blocking\": true, \"cause\": \"%s\", " +
                "\"content\": {\"$composer\": {\"openwhisk\": {\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.2.252:6379\"}}, " +
                "\"$scheduler\": {\"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 10, \"memory\": 256, \"timeout\": 60000, " +
                "\"userMemory\": 2048}, \"overload\": false, \"priority\": %d, \"target\": \"invoker1\", \"cmpLength\": 1}}, \"initArgs\": [], " +
                "\"lockedArgs\": {}, \"revision\": \"2-5e03f90f3b32b50df7c5cb3b36660122\", \"rootControllerIndex\": " +
                "{\"asString\": \"0\", \"instanceType\": \"controller\"}, \"transid\": [\"Q1w7PbuBTN2Ie80xVKNWT1xmI2FWNsDC\", " +
                "1617722325467, [\"PNu7ZGEuJVY77EqCurRjt21AL3FvqdgM\", 1617722325273]], \"user\": {\"authkey\": {\"api_key\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";

        final Activation activation1 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 1), Activation.class);
        final Activation activation2 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 2), Activation.class);
        final Activation activation3 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 3), Activation.class);
        final Collection<Activation> activations = new ArrayList<>() {{
            add(activation2);
            add(activation1);
            add(activation3);
        }};

        final int compositionLimit = 2;
        final IPolicy policy = PolicyFactory.createPolicy(Policy.RUNNING_COMPOSITION_PQFIFO);
        ((RunningCompositionPQFIFOPolicy) policy).setRunningCompositionsLimit(compositionLimit);
        final Queue<? extends ISchedulable> invocationQueue = policy.apply(activations);

        assertEquals(2, invocationQueue.size(), "Invocation queue size must be 2");
        assertEquals(activation3, invocationQueue.poll(), "Applied policy does not have the correct priority level, should be 3");
        assertEquals(activation2, invocationQueue.poll(), "Applied policy does not have the correct priority level, should be 2");

        // activationId: 5d04d8d3d7424c9484d8d3d742dc94b6
        final String blockingCompletionRecord = "{\"instance\":{\"instance\":0,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-0\"," +
                "\"userMemory\":\"2147483648 B\"},\"isSystemError\":false,\"response\":{\"activationId\":\"81785d241fe94fd2b85d241fe9bfd24e\"," +
                "\"annotations\":[{\"key\":\"causedBy\",\"value\":\"sequence\"},{\"key\":\"path\",\"value\":\"guest/cmp\"}," +
                "{\"key\":\"waitTime\",\"value\":960},{\"key\":\"kind\",\"value\":\"nodejs:10\"}," +
                "{\"key\":\"timeout\",\"value\":false},{\"key\":\"limits\",\"value\":" +
                "{\"concurrency\":3,\"logs\":10,\"memory\":256,\"timeout\":60000}}," +
                "{\"key\":\"initTime\",\"value\":530}],\"cause\":\"%s\",\"duration\":613,\"end\":1613386872211," +
                "\"logs\":[],\"name\":\"cmp\",\"namespace\":\"guest\",\"publish\":false,\"response\":{\"result\":{\"action\":\"/_/fn1\"," +
                "\"method\":\"action\",\"params\":{\"$scheduler\":{\"duration\":7,\"limits\":{\"concurrency\":3,\"memory\":256," +
                "\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"}," +
                "\"sleep_time\":5,\"user\":\"Kira\"},\"state\":{\"$composer\":{\"resuming\":true,\"session\":\"81785d241fe94fd2b85d241fe9bfd24e\"," +
                "\"stack\":[],\"state\":2}}},\"size\":335,\"statusCode\":0},\"start\":1613386871598,\"subject\":\"guest\",\"version\":\"0.0.2\"}," +
                "\"transid\":[\"Bh2gbKpjKcjV0Jj1GSlvv8cxQ8berti7\",1613386870556,[\"G6yqpDPu9WsjFdpZxIIS1CEe4sRzovMY\",1613386870514]]}";
        final BlockingCompletion blockingCompletionActivation = objectMapper.readValue(String.format(blockingCompletionRecord, activation3.getCause()), BlockingCompletion.class);
        final Collection<BlockingCompletion> events = new ArrayList<>() {{
            add(blockingCompletionActivation);
        }};
        final Queue<? extends IConsumable> updateInvocationQueue = policy.update(events);

        assertEquals(1, updateInvocationQueue.size());
        assertEquals(activation1, updateInvocationQueue.poll());
    }

    @Test
    public void whenTimeoutTriggers_PolicyRemoveTracedCompositionCorrectly() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // activationId: d558b9d3150a467298b9d3150a067276
        // cause: 2dd491e759e747919491e759e7b791e9
        final String activationRecord = "{\"action\": {\"name\": \"img_man\", \"path\": \"guest\", \"version\": \"0.0.2\"}, " +
                "\"activationId\": \"%s\", \"blocking\": true, \"cause\": \"%s\", " +
                "\"content\": {\"$composer\": {\"openwhisk\": {\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.2.252:6379\"}}, " +
                "\"$scheduler\": {\"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 10, \"memory\": 256, \"timeout\": 60000, " +
                "\"userMemory\": 2048}, \"overload\": false, \"priority\": %d, \"target\": \"invoker1\"}}, \"initArgs\": [], " +
                "\"lockedArgs\": {}, \"revision\": \"2-5e03f90f3b32b50df7c5cb3b36660122\", \"rootControllerIndex\": " +
                "{\"asString\": \"0\", \"instanceType\": \"controller\"}, \"transid\": [\"Q1w7PbuBTN2Ie80xVKNWT1xmI2FWNsDC\", " +
                "1617722325467, [\"PNu7ZGEuJVY77EqCurRjt21AL3FvqdgM\", 1617722325273]], \"user\": {\"authkey\": {\"api_key\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";
        // activationId: 5d04d8d3d7424c9484d8d3d742dc94b6
        final String eventConductorRecord = "{\"body\": {\"activationId\": \"%s\", \"conductor\": true, " +
                "\"duration\": 10304, \"initTime\": 0, \"kind\": \"sequence\", \"memory\": 256, \"name\": \"guest/img_man\", " +
                "\"statusCode\": 0, \"waitTime\": 4}, \"eventType\": \"Activation\", \"namespace\": \"guest\", " +
                "\"source\": \"controller0\", \"subject\": \"guest\", \"timestamp\": 1617722408277, \"userId\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";

        final Activation activation3 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 3), Activation.class);
        final Collection<Activation> activations1 = new ArrayList<>() {{
            add(activation3);
        }};
        final Activation activation1 = activation3.with(1);
        final Collection<Activation> activations2 = new ArrayList<>() {{
            add(activation1);
        }};
        final ActivationEvent activationEvent = objectMapper.readValue(String.format(eventConductorRecord, activation3.getActivationId()), ActivationEvent.class);
        final Collection<ActivationEvent> events = new ArrayList<>() {{
           add(activationEvent);
        }};

        final int compositionLimit = 2;
        final IPolicy policy = PolicyFactory.createPolicy(Policy.RUNNING_COMPOSITION_PQFIFO);
        ((RunningCompositionPQFIFOPolicy) policy).setRunningCompositionsLimit(compositionLimit);
        final Queue<? extends ISchedulable> invocationQueue1 = policy.apply(activations1);
        // force composition completion
        policy.update(events);
        final Queue<? extends ISchedulable> invocationQueue2 = policy.apply(activations2);

        assertEquals(1, invocationQueue1.size(), "Invocation queue size must be 1");
        assertEquals(1, invocationQueue2.size(), "Invocation queue size must be 1");
        assertEquals(activation3, invocationQueue1.poll());
        assertEquals(activation1, invocationQueue2.poll());
    }

    @Test
    public void whenTraceableCompositionArrives_PolicyTraceCorrectly() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // activationId: d558b9d3150a467298b9d3150a067276
        // cause: 2dd491e759e747919491e759e7b791e9
        final String activationRecord = "{\"action\": {\"name\": \"img_man\", \"path\": \"guest\", \"version\": \"0.0.2\"}, " +
                "\"activationId\": \"%s\", \"blocking\": true, \"cause\": \"%s\", " +
                "\"content\": {\"$composer\": {\"openwhisk\": {\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.2.252:6379\"}}, " +
                "\"$scheduler\": {\"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 10, \"memory\": 256, \"timeout\": 60000, " +
                "\"userMemory\": 2048}, \"overload\": false, \"priority\": %d, \"target\": \"invoker1\"}}, \"initArgs\": [], " +
                "\"lockedArgs\": {}, \"revision\": \"2-5e03f90f3b32b50df7c5cb3b36660122\", \"rootControllerIndex\": " +
                "{\"asString\": \"0\", \"instanceType\": \"controller\"}, \"transid\": [\"Q1w7PbuBTN2Ie80xVKNWT1xmI2FWNsDC\", " +
                "1617722325467, [\"PNu7ZGEuJVY77EqCurRjt21AL3FvqdgM\", 1617722325273]], \"user\": {\"authkey\": {\"api_key\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";

        final Activation activation3 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 3), Activation.class);
        final Collection<Activation> activations1 = new ArrayList<>() {{
            add(activation3);
        }};
        final Activation activation1 = activation3.with(1);
        final Collection<Activation> activations2 = new ArrayList<>() {{
            add(activation1);
        }};

        final int compositionLimit = 2;
        // NOTE: to pass this test, must be modified timem limit hardcoded within policy
        final IPolicy policy = PolicyFactory.createPolicy(Policy.RUNNING_COMPOSITION_PQFIFO);
        ((RunningCompositionPQFIFOPolicy) policy).setRunningCompositionsLimit(compositionLimit);
        final Queue<? extends ISchedulable> invocationQueue1 = policy.apply(activations1);
        final Activation activationQueue1 = (Activation) invocationQueue1.poll();
        final Queue<? extends ISchedulable> invocationQueue2 = policy.apply(activations2);
        final Activation activationQueue2 = (Activation) invocationQueue2.poll();

        assertEquals(0, invocationQueue1.size(), "Invocation queue size must be 0");
        assertEquals(0, invocationQueue2.size(), "Invocation queue size must be 0");
        assertEquals(activation3, activationQueue1);
        assertEquals(activation3.getPriority(), activationQueue1.getPriority());
        assertEquals(activation1, activationQueue2);
        assertEquals(activation3.getPriority(), activationQueue2.getPriority());
    }

    @Test
    public void whenNonConductorEvent_PolicyUpdateDoNothing() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // activationId: d558b9d3150a467298b9d3150a067276
        // cause: 2dd491e759e747919491e759e7b791e9
        final String activationRecord = "{\"action\": {\"name\": \"img_man\", \"path\": \"guest\", \"version\": \"0.0.2\"}, " +
                "\"activationId\": \"%s\", \"blocking\": true, \"cause\": \"%s\", " +
                "\"content\": {\"$composer\": {\"openwhisk\": {\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.2.252:6379\"}}, " +
                "\"$scheduler\": {\"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 10, \"memory\": 256, \"timeout\": 60000, " +
                "\"userMemory\": 2048}, \"overload\": false, \"priority\": %d, \"target\": \"invoker1\"}}, \"initArgs\": [], " +
                "\"lockedArgs\": {}, \"revision\": \"2-5e03f90f3b32b50df7c5cb3b36660122\", \"rootControllerIndex\": " +
                "{\"asString\": \"0\", \"instanceType\": \"controller\"}, \"transid\": [\"Q1w7PbuBTN2Ie80xVKNWT1xmI2FWNsDC\", " +
                "1617722325467, [\"PNu7ZGEuJVY77EqCurRjt21AL3FvqdgM\", 1617722325273]], \"user\": {\"authkey\": {\"api_key\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";
        // activationId: 516e336218db41a5ae336218dbb1a575
        final String eventNonConductorRecord = "{\"body\": {\"activationId\": \"%s\", \"causedBy\": \"sequence\", " +
                "\"conductor\": false, \"duration\": 33, \"initTime\": 0, \"kind\": \"nodejs:10\", \"memory\": 256, \"name\": \"guest/img_man\", " +
                "\"size\": 406, \"statusCode\": 0, \"waitTime\": 837}, \"eventType\": \"Activation\", \"namespace\": \"guest\", " +
                "\"source\": \"invoker0\", \"subject\": \"guest\", \"timestamp\": 1617722326693, \"userId\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";

        final Activation activation1 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 1), Activation.class);
        final Activation activation2 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 2), Activation.class);
        final Activation activation3 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 3), Activation.class);
        final Collection<Activation> activations = new ArrayList<>() {{
            add(activation2);
            add(activation1);
            add(activation3);
        }};
        final ActivationEvent activationEvent = objectMapper.readValue(String.format(eventNonConductorRecord, activation3.getActivationId()), ActivationEvent.class);
        final Collection<ActivationEvent> events = new ArrayList<>() {{
            add(activationEvent);
        }};

        final int compositionLimit = 2;
        final IPolicy policy = PolicyFactory.createPolicy(Policy.RUNNING_COMPOSITION_PQFIFO);
        ((RunningCompositionPQFIFOPolicy) policy).setRunningCompositionsLimit(compositionLimit);
        final Queue<? extends ISchedulable> invocationQueue = policy.apply(activations);

        assertEquals(2, invocationQueue.size(), "Invocation queue size must be 2");
        assertEquals(activation3, invocationQueue.poll());
        assertEquals(activation2, invocationQueue.poll());

        final Queue<? extends IConsumable> updateInvocationQueue = policy.update(events);;

        assertEquals(0, updateInvocationQueue.size(), "Invocation queue size must be 0");
    }

    @Test
    public void whenComponentActivationArrives_thenLetItGoIfCauseTraced() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // activationId: d558b9d3150a467298b9d3150a067276
        // cause: 2dd491e759e747919491e759e7b791e9
        final String activationRecord = "{\"action\": {\"name\": \"img_man\", \"path\": \"guest\", \"version\": \"0.0.2\"}, " +
                "\"activationId\": \"%s\", \"blocking\": true, \"cause\": \"%s\", " +
                "\"content\": {\"$composer\": {\"openwhisk\": {\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.2.252:6379\"}}, " +
                "\"$scheduler\": {\"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 10, \"memory\": 256, \"timeout\": 60000, " +
                "\"userMemory\": 2048}, \"overload\": false, \"priority\": %d, \"target\": \"invoker1\"}}, \"initArgs\": [], " +
                "\"lockedArgs\": {}, \"revision\": \"2-5e03f90f3b32b50df7c5cb3b36660122\", \"rootControllerIndex\": " +
                "{\"asString\": \"0\", \"instanceType\": \"controller\"}, \"transid\": [\"Q1w7PbuBTN2Ie80xVKNWT1xmI2FWNsDC\", " +
                "1617722325467, [\"PNu7ZGEuJVY77EqCurRjt21AL3FvqdgM\", 1617722325273]], \"user\": {\"authkey\": {\"api_key\": " +
                "\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"}, " +
                "\"limits\": {}, \"namespace\": {\"name\": \"guest\", \"uuid\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}, " +
                "\"rights\": [\"READ\", \"PUT\", \"DELETE\", \"ACTIVATE\"], \"subject\": \"guest\"}}";

        final Activation activation3 = objectMapper.readValue(String.format(activationRecord, UUID.randomUUID(), UUID.randomUUID(), 3), Activation.class);
        final Activation activation1 = activation3.with(1);
        final Collection<ISchedulable> activations = new ArrayList<>() {{
            add(activation3);
            add(activation1);
        }};

        final int compositionLimit = 1;
        final IPolicy policy = PolicyFactory.createPolicy(Policy.RUNNING_COMPOSITION_PQFIFO);
        ((RunningCompositionPQFIFOPolicy) policy).setRunningCompositionsLimit(compositionLimit);
        final Queue<? extends ISchedulable> invocationQueue = policy.apply(activations);

        assertEquals(2, invocationQueue.size(), "Invocation queue size must be 2");
        assertEquals(activation3, invocationQueue.poll());
        assertEquals(activation1, invocationQueue.poll());
    }

}