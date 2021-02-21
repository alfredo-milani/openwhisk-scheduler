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

}