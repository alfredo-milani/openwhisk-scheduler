package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Event implements IConsumable {

    /*
    {
    "body": {
        "metricName": "ConcurrentInvocations",
        "metricValue": 1
      },
      "eventType": "Metric",
      "namespace": "guest",
      "source": "controller0",
      "subject": "guest",
      "timestamp": 1606353576088,
      "userId": "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
    }
     */

    /*
    {
   "body":{
          "activationId":"841e3f99f40346429e3f99f40396428f",
          "causedBy":"sequence",
          "conductor":false,
          "duration":628,
          "initTime":575,
          "kind":"nodejs:10",
          "memory":256,
          "name":"guest/cmp",
          "size":184,
          "statusCode":0,
          "waitTime":639
       },
       "eventType":"Activation",
       "namespace":"guest",
       "source":"invoker0",
       "subject":"guest",
       "timestamp":1609870677713,
       "userId":"23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
    }
     */

    // OPTIMIZE - Body class should be of two types: Activation and Metric.
    //  For simplicity, for now, there is only one class.
    private final Body body;
    private final String eventType;
    private final String namespace;
    private final String source;
    private final String subject;
    private final Long timestamp;
    private final String userId;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Event(@JsonProperty("body") Body body, @JsonProperty("eventType") String eventType,
                 @JsonProperty("namespace") String namespace, @JsonProperty("source") String source,
                 @JsonProperty("subject") String subject, @JsonProperty("timestamp") Long timestamp,
                 @JsonProperty("userId") String userId) {
        this.body = body;
        this.eventType = eventType;
        this.namespace = namespace;
        this.source = source;
        this.subject = subject;
        this.timestamp = timestamp;
        this.userId = userId;
    }

    public Body getBody() {
        return body;
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
    public String toString() {
        return "Event{" +
                "body=" + body +
                ", eventType='" + eventType + '\'' +
                ", namespace='" + namespace + '\'' +
                ", source='" + source + '\'' +
                ", subject='" + subject + '\'' +
                ", timestamp=" + timestamp +
                ", userId='" + userId + '\'' +
                '}';
    }

}