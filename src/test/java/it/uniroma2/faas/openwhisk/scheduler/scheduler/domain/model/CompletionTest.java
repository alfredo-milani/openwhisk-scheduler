package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    public void isSchedulerDurationAlwaysLowerEqualThanWaitTime() throws IOException {
        final String filename = "/Volumes/Data/Projects/FaaS/OpenWhisk/openwhisk-scheduler/src/test/res/domain/completed0.txt";
        final ObjectMapper objectMapper = new ObjectMapper();
        final Collection<BlockingCompletion> completions;

        try (final Stream<String> stream = Files.lines(Paths.get(filename))) {
            completions = stream
                    .map(s -> {
                        try {
                            return objectMapper.readValue(s, BlockingCompletion.class);
                        } catch (JsonProcessingException e0) {
                            e0.printStackTrace();
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        final List<Integer> waitTimeCompletions = completions.stream()
                .map(completion -> completion.getResponse().getAnnotations())
                .flatMap(annotationList -> annotationList.stream()
                        .filter(annotation -> annotation.get("key").equals("waitTime"))
                        .map(waitTime -> (Integer) waitTime.get("value"))
                )
                .filter(Objects::nonNull)
                .collect(toList());
        final List<Integer> schedulerDurationCompletions = completions.stream()
                .map(completion -> completion.getResponse().getResult().getResult().get("$scheduler"))
                .map(scheduler -> ((Map<String, Integer>) scheduler).get("duration"))
                .collect(toList());

        assertEquals(completions.size(), waitTimeCompletions.size(), "Wait time annotations must be equals to completions number.");
        assertEquals(completions.size(), schedulerDurationCompletions.size(), "Scheduler duration objects must be equals to completions number.");

        IntStream.range(0, completions.size())
                .forEach( i -> assertTrue(waitTimeCompletions.get(i) >= schedulerDurationCompletions.get(i),
                        String.format("Wait time %d is not >= to %d.", waitTimeCompletions.get(i), schedulerDurationCompletions.get(i))));
    }

    @Test
    public void areErrorCompletionDeserializedCorrectly() {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String errorInvokerHealthTestActionCompletionRecord = "{\"instance\": {\"instance\": 2, \"instanceType\": \"invoker\", \"uniqueName\": \"owdev-invoker-1\", \"userMemory\": \"2147483648 B\"}, \"isSystemError\": true, \"response\": \"ba418ba98c914fc7818ba98c916fc75b\", \"transid\": [\"sid_invokerHealth\", 1614175891255]}";
        final String errorOnContainerImageCompletionRecord = "{\"instance\": {\"instance\": 0, \"instanceType\": \"invoker\", \"uniqueName\": \"owdev-invoker-2\", \"userMemory\": \"2147483648 B\"}, \"isSystemError\": true, \"response\": {\"activationId\": \"607d5b9094744892bd5b909474489267\", \"annotations\": [{\"key\": \"path\", \"value\": \"guest/sleep\"}, {\"key\": \"waitTime\", \"value\": 61609}, {\"key\": \"kind\", \"value\": \"python:3\"}, {\"key\": \"timeout\", \"value\": false}, {\"key\": \"limits\", \"value\": {\"concurrency\": 1, \"logs\": 10, \"memory\": 256, \"timeout\": 60000}}], \"duration\": 0, \"end\": 1614176278669, \"logs\": [], \"name\": \"sleep\", \"namespace\": \"guest\", \"publish\": false, \"response\": {\"result\": {\"error\": \"Failed to run container with image 'openwhisk/python3action:1.15.0'.\"}, \"statusCode\": 3}, \"start\": 1614176278669, \"subject\": \"guest\", \"version\": \"0.0.1\"}, \"transid\": [\"jIAernxDeXgY2nGon66c52qB5hP5OkMc\", 1614176217060]}";

        assertDoesNotThrow(() -> System.out.println(
                objectMapper.readValue(errorInvokerHealthTestActionCompletionRecord, FailureCompletion.class)));
        assertDoesNotThrow(() -> System.out.println(
                objectMapper.readValue(errorOnContainerImageCompletionRecord, BlockingCompletion.class)));
    }

    @Test
    public void areResultErrorDeserializedCorrectly() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String resultErrorCompletion = "{\"instance\": {\"instance\": 0, \"instanceType\": \"invoker\", \"uniqueName\": \"owdev-invoker-1\", \"userMemory\": \"2147483648 B\"}, \"isSystemError\": false, \"response\": {\"activationId\": \"b6466896b5d443dd866896b5d423dd38\", \"annotations\": [{\"key\": \"causedBy\", \"value\": \"sequence\"}, {\"key\": \"path\", \"value\": \"guest/img_man\"}, {\"key\": \"waitTime\", \"value\": 5620}, {\"key\": \"kind\", \"value\": \"nodejs:10\"}, {\"key\": \"timeout\", \"value\": false}, {\"key\": \"limits\", \"value\": {\"concurrency\": 5, \"logs\": 10, \"memory\": 256, \"timeout\": 60000}}], \"cause\": \"68864a98136a4cdf864a98136a1cdf8c\", \"duration\": 8, \"end\": 1629143743940, \"logs\": [], \"name\": \"img_man\", \"namespace\": \"guest\", \"publish\": false, \"response\": {\"result\": {\"error\": \"Failed to provision resources to run the action.\"}, \"size\": 60, \"statusCode\": 1}, \"start\": 1629143743932, \"subject\": \"guest\", \"version\": \"0.0.2\"}, \"transid\": [\"6CQJT3Xk78KOM04Q3iuTDWG5WWRDGEVN\", 1629143738312, [\"H7yAAA9QDwZN5i3G99aoTBzHSeFb2F90\", 1629143723290, [\"BCgz5ZIWPBOTQQl7UYJtRvEzKFGUOyFT\", 1629143272685, [\"Ij6cQ63fy1XBnWhHptdnSAXeAnuXAqlh\", 1629143272684]]]]}";
        final BlockingCompletion blockingCompletionResultError = objectMapper.readValue(resultErrorCompletion, BlockingCompletion.class);

        final String resultCompletion = "{\"instance\": {\"instance\": 0, \"instanceType\": \"invoker\", \"uniqueName\": \"owdev-invoker-1\", \"userMemory\": \"2147483648 B\"}, \"isSystemError\": false, \"response\": {\"activationId\": \"d624859151bc45f0a4859151bcf5f056\", \"annotations\": [{\"key\": \"causedBy\", \"value\": \"sequence\"}, {\"key\": \"path\", \"value\": \"guest/img_man\"}, {\"key\": \"waitTime\", \"value\": 5701}, {\"key\": \"kind\", \"value\": \"nodejs:10\"}, {\"key\": \"timeout\", \"value\": false}, {\"key\": \"limits\", \"value\": {\"concurrency\": 5, \"logs\": 10, \"memory\": 256, \"timeout\": 60000}}], \"cause\": \"63449cfc76f9452d849cfc76f9952db3\", \"duration\": 11, \"end\": 1629143743943, \"logs\": [], \"name\": \"img_man\", \"namespace\": \"guest\", \"publish\": false, \"response\": {\"result\": {\"action\": \"/_/mirror\", \"method\": \"action\", \"params\": {\"$scheduler\": {\"cmpLength\": 7, \"duration\": 0, \"kind\": \"nodejs:10\", \"limits\": {\"concurrency\": 5, \"memory\": 256, \"timeout\": 60000, \"userMemory\": 2048}, \"priority\": 3, \"target\": \"invoker0\"}, \"in_location\": \"faeda2d747fb4e1fada2d747fb8e1fde\", \"in_type\": \"redis\", \"out_location\": \"faeda2d747fb4e1fada2d747fb8e1fde\", \"out_type\": \"redis\"}, \"state\": {\"$composer\": {\"openwhisk\": {\"ignore_certs\": true}, \"redis\": {\"uri\": \"redis://10.64.0.53:6379\"}, \"resuming\": true, \"session\": \"3b4879376a1441ea8879376a1461eaf9\", \"stack\": [], \"state\": 3}}}, \"size\": 537, \"statusCode\": 0}, \"start\": 1629143743932, \"subject\": \"guest\", \"version\": \"0.0.2\"}, \"transid\": [\"8RotVbzIl6e3c5ReedAYwRI85mF11VW8\", 1629143738231, [\"tgS8EaOUVtf6qK4FnvZjX7Y0HXDHvKwm\", 1629143723210, [\"UfRfujhUkkrBaQQdgpK41fBO2x5LCivx\", 1629143272613, [\"foZydqZDJfdPwB30KG6NWOaqyAcFNT0I\", 1629143272612]]]]}";
        final BlockingCompletion blockingCompletionResult = objectMapper.readValue(resultCompletion, BlockingCompletion.class);

        assertTrue(blockingCompletionResultError.getResponse().getResult().getError(), "Completion must have result error.");
        assertFalse(blockingCompletionResult.getResponse().getResult().getError());
    }

}