package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class Response {

    /*
    {
        "response": {
        "activationId": "a5fd98ab2e4d4f6ebd98ab2e4dcf6e4d",
        "annotations": [
          {
            "key": "path",
            "value": "guest/echo"
          },
          {
            "key": "waitTime",
            "value": 416
          },
          {
            "key": "kind",
            "value": "nodejs:10"
          },
          {
            "key": "timeout",
            "value": false
          },
          {
            "key": "limits",
            "value": {
              "concurrency": 10,
              "logs": 10,
              "memory": 256,
              "timeout": 60000
            }
          }
        ],
        "duration": 12,
        "end": 1612222406352,
        "logs": [],
        "name": "echo",
        "namespace": "guest",
        "publish": false,
        "response": {
          "result": {
            "input": {
              "$scheduler": {
                "limits": {
                  "concurrency": 10,
                  "memory": 256,
                  "timeout": 60000
                },
                "target": "invoker0"
              },
              "scheduler": {
                "priority": 0,
                "target": "invoker0"
              },
              "test": 10000
            }
          },
          "size": 163,
          "statusCode": 0
        }
    }
     */

    private final String activationId;
    private final List<Map<String, Object>> annotations;
    private final Long duration;
    private final Long end;
    private final List<String> logs;
    private final String name;
    private final String namespace;
    private final Boolean publish;
    private final Result result;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Response(@JsonProperty("activationId") String activationId, @JsonProperty("annotations") List<Map<String, Object>> annotations,
                    @JsonProperty("duration") Long duration, @JsonProperty("end") Long end,
                    @JsonProperty("logs") List<String> logs, @JsonProperty("name") String name,
                    @JsonProperty("namespace") String namespace, @JsonProperty("publish") Boolean publish,
                    @JsonProperty("response") Result result) {
        this.activationId = activationId;
        this.annotations = annotations;
        this.duration = duration;
        this.end = end;
        this.logs = logs;
        this.name = name;
        this.namespace = namespace;
        this.publish = publish;
        this.result = result;
    }

    public String getActivationId() {
        return activationId;
    }

    public List<Map<String, Object>> getAnnotations() {
        return annotations;
    }

    public Long getDuration() {
        return duration;
    }

    public Long getEnd() {
        return end;
    }

    public List<String> getLogs() {
        return logs;
    }

    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public Boolean getPublish() {
        return publish;
    }

    public Result getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "Response{" +
                "activationId='" + activationId + '\'' +
                ", annotations=" + annotations +
                ", duration=" + duration +
                ", end=" + end +
                ", logs=" + logs +
                ", name='" + name + '\'' +
                ", namespace='" + namespace + '\'' +
                ", publish=" + publish +
                ", result=" + result +
                '}';
    }

}