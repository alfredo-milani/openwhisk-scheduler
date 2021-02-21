package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Activation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TracerSchedulerTest {

    @Test
    public void areActivationsTracedCorrectly() throws IOException {
         final String filename = "/Volumes/Data/Projects/FaaS/OpenWhisk/openwhisk-scheduler/src/test/res/tracer_scheduler/scheduler_compositions.txt";
//        final String filename = "/Volumes/Data/Projects/FaaS/OpenWhisk/openwhisk-scheduler/benchmark/18/data/scheduler.txt";
        final ObjectMapper objectMapper = new ObjectMapper();
        final Collection<Activation> activations;

        try (final Stream<String> stream = Files.lines(Paths.get(filename))) {
            activations = stream
                    .map(s -> {
                        try {
                            return objectMapper.readValue(s, Activation.class);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        assertEquals(2L, activations.stream().filter(a -> a.getCause() == null).count(),
                "Activations count is not equal to 2 (that is the number of invokerTestHealthAction).");

        assertTrue(activations.stream()
                        .filter(a -> a.getCause() != null)
                        .collect(groupingBy(Activation::getCause))
                        .values().stream()
                        .allMatch(l -> l.size() == 7),
                "All activations having same cause (belonging to the same chain) must be exactly 7."
        );

        assertTrue(activations.stream()
                        .filter(a -> a.getCause() != null)
                        .collect(groupingBy(Activation::getCause))
                        .values().stream()
                        .allMatch(l -> l.stream().map(Activation::getPriority).distinct().count() == 1),
                "All singles compositions invocation must generate all activations with same priority level."
        );
    }

}