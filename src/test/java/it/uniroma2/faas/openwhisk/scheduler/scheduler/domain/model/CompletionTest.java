package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CompletionTest {

    @Test
    public void givenCompletionFromSimpleAction_thenCompletionIsDeserializedCorrectly() {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String blockingCompletionRecordFromSimpleAction = "{\"instance\":{\"instance\":0,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-0\",\"userMemory\":\"2147483648 B\"},\"isSystemError\":false,\"response\":{\"activationId\":\"50ca4ef8bb984b778a4ef8bb988b771a\",\"annotations\":[{\"key\":\"path\",\"value\":\"guest/sleep_one\"},{\"key\":\"waitTime\",\"value\":6326},{\"key\":\"kind\",\"value\":\"python:3\"},{\"key\":\"timeout\",\"value\":false},{\"key\":\"limits\",\"value\":{\"concurrency\":2,\"logs\":10,\"memory\":256,\"timeout\":60000}},{\"key\":\"initTime\",\"value\":35}],\"duration\":2046,\"end\":1613386579997,\"logs\":[],\"name\":\"sleep_one\",\"namespace\":\"guest\",\"publish\":false,\"response\":{\"result\":{\"sleep_one\":{\"$scheduler\":{\"duration\":50,\"limits\":{\"concurrency\":2,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"sleep\":2}},\"size\":179,\"statusCode\":0},\"start\":1613386577951,\"subject\":\"guest\",\"version\":\"0.0.1\"},\"transid\":[\"qIegNpYovh58teI9Vy6wmyvWqKwZL4ha\",1613386571622]}";

        assertDoesNotThrow(() -> System.out.println(
                objectMapper.readValue(blockingCompletionRecordFromSimpleAction, BlockingCompletion.class)));
    }

    @Test
    public void givenCompletionFromComposition_thenCompletionIsDeserializedCorrectly() {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String blockingCompletionRecordFromComposition = "{\"instance\":{\"instance\":0,\"instanceType\":\"invoker\",\"uniqueName\":\"owdev-invoker-0\",\"userMemory\":\"2147483648 B\"},\"isSystemError\":false,\"response\":{\"activationId\":\"81785d241fe94fd2b85d241fe9bfd24e\",\"annotations\":[{\"key\":\"causedBy\",\"value\":\"sequence\"},{\"key\":\"path\",\"value\":\"guest/cmp\"},{\"key\":\"waitTime\",\"value\":960},{\"key\":\"kind\",\"value\":\"nodejs:10\"},{\"key\":\"timeout\",\"value\":false},{\"key\":\"limits\",\"value\":{\"concurrency\":3,\"logs\":10,\"memory\":256,\"timeout\":60000}},{\"key\":\"initTime\",\"value\":530}],\"cause\":\"802c89ce6b414636ac89ce6b4196361f\",\"duration\":613,\"end\":1613386872211,\"logs\":[],\"name\":\"cmp\",\"namespace\":\"guest\",\"publish\":false,\"response\":{\"result\":{\"action\":\"/_/fn1\",\"method\":\"action\",\"params\":{\"$scheduler\":{\"duration\":7,\"limits\":{\"concurrency\":3,\"memory\":256,\"timeout\":60000,\"userMemory\":2048},\"overload\":false,\"priority\":0,\"target\":\"invoker0\"},\"sleep_time\":5,\"user\":\"Kira\"},\"state\":{\"$composer\":{\"resuming\":true,\"session\":\"81785d241fe94fd2b85d241fe9bfd24e\",\"stack\":[],\"state\":2}}},\"size\":335,\"statusCode\":0},\"start\":1613386871598,\"subject\":\"guest\",\"version\":\"0.0.2\"},\"transid\":[\"Bh2gbKpjKcjV0Jj1GSlvv8cxQ8berti7\",1613386870556,[\"G6yqpDPu9WsjFdpZxIIS1CEe4sRzovMY\",1613386870514]]}";

        assertDoesNotThrow(() -> System.out.println(
                objectMapper.readValue(blockingCompletionRecordFromComposition, BlockingCompletion.class)));
    }

}