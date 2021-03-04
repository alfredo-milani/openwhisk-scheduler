package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.BlockingCompletion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.NonBlockingCompletion;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

public class ShortestJobFirstPolicyTest {

    @Test
    public void givenInputCollection_thenPolicySortCorrectly() throws Exception {
        final Random random = new Random();
        final ObjectMapper objectMapper = new ObjectMapper();

        final String blockingCompletionRecord = "{\"instance\":{\"instance\":0,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-0\",\"userMemory\":\"2147483648 B\"},\"isSystemError\":false,\"response\":{\"activationId\":\"%s\",\"annotations\":[{\"key\":\"path\",\"value\":\"guest/sleep_one\"},{\"key\":\"waitTime\",\"value\":4545},{\"key\":\"kind\",\"value\":\"python:3\"},{\"key\":\"timeout\",\"value\":false},{\"key\":\"limits\",\"value\":{\"concurrency\":5,\"logs\":10,\"memory\":256,\"timeout\":60000}},{\"key\":\"initTime\",\"value\":28}],\"duration\":%d,\"end\":1612889796543,\"logs\":[],\"name\":\"%s\",\"namespace\":\"guest\",\"publish\":false,\"response\":{\"result\":{\"sleep_one\":{\"$scheduler\":{\"limits\":{\"concurrency\":5,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"target\":\"invoker0\"},\"scheduler\":{\"priority\":0,\"target\":\"invoker0\"},\"sleep\":1}},\"size\":199,\"statusCode\":0},\"start\":1612889795506,\"subject\":\"guest\",\"version\":\"0.0.1\"},\"transid\":[\"iud25hHHjA0mtOTri9nKePhEnDhcMpH2\",1612889790960]}";
        final String nonBlockingCompletionRecord = "{\"activationId\":\"%s\",\"instance\":{\"instance\":0,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-0\",\"userMemory\":\"2147483648 B\"},\"isSystemError\":false,\"transid\":[\"MSCztINkcSGlhgeQT6H7YJAPaDVNO0nK\",1612935312054]}";



        final Collection<Completion> completions = new ArrayDeque<>(100);
        for (int i = 0; i < 25; ++i) {
            final BlockingCompletion blockingCompletion = objectMapper.readValue(
                    String.format(blockingCompletionRecord, UUID.randomUUID(), generatingRandomLongBounded(500L, 20_000L), "action" + i),
                    BlockingCompletion.class
            );
            final NonBlockingCompletion nonBlockingCompletion = objectMapper.readValue(
                    String.format(nonBlockingCompletionRecord, UUID.randomUUID()),
                    NonBlockingCompletion.class
            );

            completions.add(blockingCompletion);
            completions.add(nonBlockingCompletion);
        }
        for (int i = 0; i < 25; ++i) {
            final BlockingCompletion blockingCompletion = objectMapper.readValue(
                    String.format(blockingCompletionRecord, UUID.randomUUID(), generatingRandomLongBounded(500L, 20_000L), "action" + i),
                    BlockingCompletion.class
            );
            final NonBlockingCompletion nonBlockingCompletion = objectMapper.readValue(
                    String.format(nonBlockingCompletionRecord, UUID.randomUUID()),
                    NonBlockingCompletion.class
            );

            completions.add(blockingCompletion);
            completions.add(nonBlockingCompletion);
        }

        final IPolicy policy = PolicyFactory.createPolicy(Policy.SHORTEST_JOB_FIRST);


        final Collection<Activation> activations = new ArrayDeque<>(25);
        final String activationRecord = "{\"action\":{\"name\":\"%s\",\"path\":\"guest\",\"version\":\"0.0.1\"},\"activationId\":\"%s\",\"blocking\":true,\"content\":{\"kTest\":\"vTest\",\"$scheduler\":{\"target\":\"invoker0\",\"priority\":1,\"limits\":{\"concurrency\":2,\"memory\":256,\"timeout\":60000,\"userMemory\":2147483648}}},\"initArgs\":[],\"lockedArgs\":{},\"revision\":\"1-ff07dcb3291545090f86e9fc1a01b5bf\",\"rootControllerIndex\":{\"asString\":\"0\",\"instanceType\":\"controller\"},\"transid\":[\"4K6K9Uoz09O5ut42VEiFI9zrOAiJX8oq\",1611192819095],\"user\":{\"authkey\":{\"api_key\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP\"},\"limits\":{},\"namespace\":{\"name\":\"guest\",\"uuid\":\"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"},\"rights\":[\"READ\",\"PUT\",\"DELETE\",\"ACTIVATE\"],\"subject\":\"guest\"}}";
        for (int i = 0; i < 25; ++i) {
            activations.add(objectMapper.readValue(
                    String.format(activationRecord, "action" + i, UUID.randomUUID()),
                    Activation.class
            ));
        }
        policy.apply(activations);
        policy.update(completions);
    }

    public long generatingRandomLongBounded(long leftLimit, long rightLimit) {
        return leftLimit + (long) (Math.random() * (rightLimit - leftLimit));
    }

}