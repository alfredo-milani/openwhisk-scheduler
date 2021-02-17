package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(names = "Metric", value = MetricEvent.class),
        @JsonSubTypes.Type(names = "Activation", value = ActivationEvent.class)
})
// @JsonInclude(JsonInclude.Include.NON_NULL)
public class Event implements IConsumable {

    /* Associated with metric
     * {
     *   "body": {
     *     "metricName": "ConcurrentInvocations",
     *     "metricValue": 7
     *   },
     *   "eventType": "Metric",
     *   "namespace": "guest",
     *   "source": "controller0",
     *   "subject": "guest",
     *   "timestamp": 1613583572894,
     *   "userId": "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
     * }
     */

    /* Associated with an activation
     * {
     *   "body": {
     *     "activationId": "fb70fb0704064336b0fb0704065336a1",
     *     "causedBy": "sequence",
     *     "conductor": false,
     *     "duration": 703,
     *     "initTime": 0,
     *     "kind": "nodejs:10",
     *     "memory": 256,
     *     "name": "guest/cmp",
     *     "size": 308,
     *     "statusCode": 0,
     *     "waitTime": 4966
     *   },
     *   "eventType": "Activation",
     *   "namespace": "guest",
     *   "source": "invoker0",
     *   "subject": "guest",
     *   "timestamp": 1613583578634,
     *   "userId": "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
     * }
     */

    private final String eventType;
    private final String namespace;
    private final String source;
    private final String subject;
    private final Long timestamp;
    private final String userId;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Event(@JsonProperty("eventType") String eventType, @JsonProperty("namespace") String namespace,
                 @JsonProperty("source") String source, @JsonProperty("subject") String subject,
                 @JsonProperty("timestamp") Long timestamp, @JsonProperty("userId") String userId) {
        this.eventType = eventType;
        this.namespace = namespace;
        this.source = source;
        this.subject = subject;
        this.timestamp = timestamp;
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSource() {
        return source;
    }

    public String getSubject() {
        return subject;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getUserId() {
        return userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(eventType, event.eventType) && Objects.equals(namespace, event.namespace) &&
                Objects.equals(source, event.source) && Objects.equals(subject, event.subject) &&
                Objects.equals(timestamp, event.timestamp) && Objects.equals(userId, event.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, namespace, source, subject, timestamp, userId);
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventType='" + eventType + '\'' +
                ", namespace='" + namespace + '\'' +
                ", source='" + source + '\'' +
                ", subject='" + subject + '\'' +
                ", timestamp=" + timestamp +
                ", userId='" + userId + '\'' +
                '}';
    }

}