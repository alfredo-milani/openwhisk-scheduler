package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EventTest {

    @Test
    public void whenEventDeserialized_thenIsOfCorrectType() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String metricEventRecord = "{\"body\": {\"metricName\": \"ConcurrentInvocations\", \"metricValue\": 9}, \"eventType\": \"Metric\", \"namespace\": \"guest\", \"source\": \"controller0\", \"subject\": \"guest\", \"timestamp\": 1613583573374, \"userId\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";
        final String activationEventRecord = "{\"body\": {\"activationId\": \"b27a9844f14c480cba9844f14c580c5f\", \"causedBy\": \"sequence\", \"conductor\": false, \"duration\": 22, \"initTime\": 0, \"kind\": \"nodejs:10\", \"memory\": 256, \"name\": \"guest/cmp\", \"size\": 308, \"statusCode\": 0, \"waitTime\": 4978}, \"eventType\": \"Activation\", \"namespace\": \"guest\", \"source\": \"invoker0\", \"subject\": \"guest\", \"timestamp\": 1613583578586, \"userId\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";
        final String conductorActivationEventRecord = "{\"body\": {\"activationId\": \"08a4b88203c241e6a4b88203c211e6e8\", \"conductor\": true, \"duration\": 10983, \"initTime\": 0, \"kind\": \"sequence\", \"memory\": 256, \"name\": \"guest/cmp\", \"statusCode\": 0, \"waitTime\": 372}, \"eventType\": \"Activation\", \"namespace\": \"guest\", \"source\": \"controller0\", \"subject\": \"guest\", \"timestamp\": 1613583604110, \"userId\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";

        final Event metricEvent = objectMapper.readValue(metricEventRecord, Event.class);
        final Event activationEvent = objectMapper.readValue(activationEventRecord, Event.class);
        final Event conductorActivationEvent = objectMapper.readValue(conductorActivationEventRecord, Event.class);

        assertTrue(metricEvent instanceof MetricEvent);
        assertTrue(activationEvent instanceof ActivationEvent);
        assertTrue(conductorActivationEvent instanceof ActivationEvent);
    }

    @Test
    public void isEventDeserializedCorrectly() {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String metricEventRecord = "{\"body\": {\"metricName\": \"ConcurrentInvocations\", \"metricValue\": 9}, \"eventType\": \"Metric\", \"namespace\": \"guest\", \"source\": \"controller0\", \"subject\": \"guest\", \"timestamp\": 1613583573374, \"userId\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";
        final String activationEventRecord = "{\"body\": {\"activationId\": \"b27a9844f14c480cba9844f14c580c5f\", \"causedBy\": \"sequence\", \"conductor\": false, \"duration\": 22, \"initTime\": 0, \"kind\": \"nodejs:10\", \"memory\": 256, \"name\": \"guest/cmp\", \"size\": 308, \"statusCode\": 0, \"waitTime\": 4978}, \"eventType\": \"Activation\", \"namespace\": \"guest\", \"source\": \"invoker0\", \"subject\": \"guest\", \"timestamp\": 1613583578586, \"userId\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";
        final String conductorActivationEventRecord = "{\"body\": {\"activationId\": \"08a4b88203c241e6a4b88203c211e6e8\", \"conductor\": true, \"duration\": 10983, \"initTime\": 0, \"kind\": \"sequence\", \"memory\": 256, \"name\": \"guest/cmp\", \"statusCode\": 0, \"waitTime\": 372}, \"eventType\": \"Activation\", \"namespace\": \"guest\", \"source\": \"controller0\", \"subject\": \"guest\", \"timestamp\": 1613583604110, \"userId\": \"23bc46b1-71f6-4ed5-8c54-816aa4f8c502\"}";

        assertDoesNotThrow(() -> System.out.println(objectMapper.readValue(metricEventRecord, Event.class)));
        assertDoesNotThrow(() -> System.out.println(objectMapper.readValue(activationEventRecord, Event.class)));
        assertDoesNotThrow(() -> System.out.println(objectMapper.readValue(conductorActivationEventRecord, Event.class)));
    }

}