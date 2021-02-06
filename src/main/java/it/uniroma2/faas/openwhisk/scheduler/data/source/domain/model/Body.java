package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Body {

    /*
    {
    "body": {
        "metricName": "ConcurrentInvocations",
        "metricValue": 1
      }
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
       }
    }
     */

    private final String activationId;
    private final String causedBy;
    private final Long duration;
    private final Long initTime;
    private final String kind;
    private final Integer memory;
    private final String name;
    private final Long size;
    private final Integer statusCode;
    private final Long waitTime;

    private final String metricName;
    private final Long metricValue;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Body(@JsonProperty("activationId") String activationId, @JsonProperty("causedBy") String causedBy,
                @JsonProperty("duration") Long duration, @JsonProperty("initTime") Long initTime,
                @JsonProperty("kind") String kind, @JsonProperty("memory") Integer memory,
                @JsonProperty("name") String name, @JsonProperty("size") Long size,
                @JsonProperty("statusCode") Integer statusCode, @JsonProperty("waitTime") Long waitTime,
                @JsonProperty("metricName") String metricName, @JsonProperty("metricValue") Long metricValue) {
        this.activationId = activationId;
        this.causedBy = causedBy;
        this.duration = duration;
        this.initTime = initTime;
        this.kind = kind;
        this.memory = memory;
        this.name = name;
        this.size = size;
        this.statusCode = statusCode;
        this.waitTime = waitTime;
        this.metricName = metricName;
        this.metricValue = metricValue;
    }

    public String getActivationId() {
        return activationId;
    }

    public String getCausedBy() {
        return causedBy;
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

    public Integer getMemory() {
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

    public String getMetricName() {
        return metricName;
    }

    public Long getMetricValue() {
        return metricValue;
    }

    @Override
    public String toString() {
        return "Body{" +
                "activationId='" + activationId + '\'' +
                ", causedBy='" + causedBy + '\'' +
                ", duration=" + duration +
                ", initTime=" + initTime +
                ", kind='" + kind + '\'' +
                ", memory=" + memory +
                ", name='" + name + '\'' +
                ", size=" + size +
                ", statusCode=" + statusCode +
                ", waitTime=" + waitTime +
                ", metricName='" + metricName + '\'' +
                ", metricValue=" + metricValue +
                '}';
    }

}