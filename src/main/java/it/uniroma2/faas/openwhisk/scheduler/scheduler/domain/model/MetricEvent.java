package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class MetricEvent extends Event {

    /*
     * {
     *   "body": {
     *     "metricName": "ConcurrentInvocations",
     *     "metricValue": 10
     *   }
     * }
     */

    private final Body body;

    public static final class Body {

        private final String metricName;
        private final Long metricValue;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        Body(@JsonProperty("metricName") String metricName, @JsonProperty("metricValue") Long metricValue) {
            this.metricName = metricName;
            this.metricValue = metricValue;
        }

        public String getMetricName() {
            return metricName;
        }

        public Long getMetricValue() {
            return metricValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Body body = (Body) o;
            return Objects.equals(metricName, body.metricName) &&
                    Objects.equals(metricValue, body.metricValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metricName, metricValue);
        }

        @Override
        public String toString() {
            return "Body{" +
                    "metricName='" + metricName + '\'' +
                    ", metricValue=" + metricValue +
                    '}';
        }

    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MetricEvent(@JsonProperty("eventType") String eventType, @JsonProperty("namespace") String namespace,
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
        MetricEvent that = (MetricEvent) o;
        return Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), body);
    }

    @Override
    public String toString() {
        return "MetricEvent{" +
                "body=" + body +
                "} " + super.toString();
    }

}