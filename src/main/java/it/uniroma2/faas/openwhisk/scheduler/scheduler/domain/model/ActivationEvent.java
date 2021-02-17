package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class ActivationEvent extends Event {

    /*
     * {
     *   "body": {
     *     "activationId": "d4127819a5674639927819a5673639cf",
     *     "causedBy": "sequence",
     *     "conductor": false,
     *     "duration": 356,
     *     "initTime": 0,
     *     "kind": "nodejs:10",
     *     "memory": 256,
     *     "name": "guest/cmp",
     *     "size": 307,
     *     "statusCode": 0,
     *     "waitTime": 4743
     *   }
     * }
     */

    private final Body body;

    public static final class Body {

        private final String activationId;
        private final String causedBy;
        private final Boolean conductor;
        private final Long duration;
        private final Long initTime;
        private final String kind;
        private final Long memory;
        private final String name;
        private final Long size;
        private final Integer statusCode;
        private final Long waitTime;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        Body(@JsonProperty("activationId") String activationId, @JsonProperty("causedBy") String causedBy,
                    @JsonProperty("conductor") Boolean conductor, @JsonProperty("duration") Long duration,
                    @JsonProperty("initTime") Long initTime, @JsonProperty("kind") String kind,
                    @JsonProperty("memory") Long memory, @JsonProperty("name") String name,
                    @JsonProperty("size") Long size, @JsonProperty("statusCode") Integer statusCode,
                    @JsonProperty("waitTime") Long waitTime) {
            this.activationId = activationId;
            this.causedBy = causedBy;
            this.conductor = conductor;
            this.duration = duration;
            this.initTime = initTime;
            this.kind = kind;
            this.memory = memory;
            this.name = name;
            this.size = size;
            this.statusCode = statusCode;
            this.waitTime = waitTime;
        }

        public String getActivationId() {
            return activationId;
        }

        public String getCausedBy() {
            return causedBy;
        }

        public Boolean getConductor() {
            return conductor;
        }

        public Long getDuration() {
            return duration;
        }

        public Long getInitTime() {
            return initTime;
        }

        public String getKind() {
            return kind;
        }

        public Long getMemory() {
            return memory;
        }

        public String getName() {
            return name;
        }

        public Long getSize() {
            return size;
        }

        public Integer getStatusCode() {
            return statusCode;
        }

        public Long getWaitTime() {
            return waitTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Body body = (Body) o;
            return Objects.equals(activationId, body.activationId) && Objects.equals(causedBy, body.causedBy) && Objects.equals(conductor, body.conductor) && Objects.equals(duration, body.duration) && Objects.equals(initTime, body.initTime) && Objects.equals(kind, body.kind) && Objects.equals(memory, body.memory) && Objects.equals(name, body.name) && Objects.equals(size, body.size) && Objects.equals(statusCode, body.statusCode) && Objects.equals(waitTime, body.waitTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(activationId, causedBy, conductor, duration, initTime, kind, memory, name, size, statusCode, waitTime);
        }

        @Override
        public String toString() {
            return "Body{" +
                    "activationId='" + activationId + '\'' +
                    ", causedBy='" + causedBy + '\'' +
                    ", conductor=" + conductor +
                    ", duration=" + duration +
                    ", initTime=" + initTime +
                    ", kind='" + kind + '\'' +
                    ", memory=" + memory +
                    ", name='" + name + '\'' +
                    ", size=" + size +
                    ", statusCode=" + statusCode +
                    ", waitTime=" + waitTime +
                    '}';
        }

    }

    public ActivationEvent(@JsonProperty("eventType") String eventType, @JsonProperty("namespace") String namespace,
                           @JsonProperty("source") String source, @JsonProperty("subject") String subject,
                           @JsonProperty("timestamp") Long timestamp, @JsonProperty("userId") String userId,
                           @JsonProperty("body") Body body) {
        super(eventType, namespace, source, subject, timestamp, userId);
        this.body = body;
    }

    public Body getBody() {
        return body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ActivationEvent that = (ActivationEvent) o;
        return Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), body);
    }

    @Override
    public String toString() {
        return "ActivationEvent{" +
                "body=" + body +
                "} " + super.toString();
    }

}