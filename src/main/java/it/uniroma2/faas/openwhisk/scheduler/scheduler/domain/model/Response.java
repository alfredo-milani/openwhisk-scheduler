package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class Response {

    /*
     *   "response": {
     *     "activationId": "50bd97db9fef433abd97db9fef933ab1",
     *     "annotations": [
     *       {
     *         "key": "path",
     *         "value": "guest/sleep_one"
     *       },
     *       {
     *         "key": "waitTime",
     *         "value": 4545
     *       },
     *       {
     *         "key": "kind",
     *         "value": "python:3"
     *       },
     *       {
     *         "key": "timeout",
     *         "value": false
     *       },
     *       {
     *         "key": "limits",
     *         "value": {
     *           "concurrency": 5,
     *           "logs": 10,
     *           "memory": 256,
     *           "timeout": 60000
     *         }
     *       },
     *       {
     *         "key": "initTime",
     *         "value": 28
     *       }
     *     ],
     *     "duration": 1037,
     *     "end": 1612889796543,
     *     "logs": [],
     *     "name": "sleep_one",
     *     "namespace": "guest",
     *     "publish": false,
     *     "response": {
     *       "result": {
     *         "sleep_one": {
     *           "$scheduler": {
     *             "limits": {
     *               "concurrency": 5,
     *               "memory": 256,
     *               "timeout": 60000,
     *               "userMemory": 2048
     *             },
     *             "overload": false,
     *             "target": "invoker0"
     *           },
     *           "scheduler": {
     *             "priority": 0,
     *             "target": "invoker0"
     *           },
     *           "sleep": 1
     *         }
     *       },
     *       "size": 199,
     *       "statusCode": 0
     *     },
     *     "start": 1612889795506,
     *     "subject": "guest",
     *     "version": "0.0.1"
     *   }
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
    private final Long start;
    private final String subject;
    private final String version;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Response(@JsonProperty("activationId") String activationId, @JsonProperty("annotations") List<Map<String, Object>> annotations,
                    @JsonProperty("duration") Long duration, @JsonProperty("end") Long end,
                    @JsonProperty("logs") List<String> logs, @JsonProperty("name") String name,
                    @JsonProperty("namespace") String namespace, @JsonProperty("publish") Boolean publish,
                    @JsonProperty("response") Result result, @JsonProperty("start") Long start,
                    @JsonProperty("subject") String subject, @JsonProperty("version") String version) {
        this.activationId = activationId;
        this.annotations = annotations;
        this.duration = duration;
        this.end = end;
        this.logs = logs;
        this.name = name;
        this.namespace = namespace;
        this.publish = publish;
        this.result = result;
        this.start = start;
        this.subject = subject;
        this.version = version;
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

    public Long getStart() {
        return start;
    }

    public String getSubject() {
        return subject;
    }

    public String getVersion() {
        return version;
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
                ", start=" + start +
                ", subject='" + subject + '\'' +
                ", version='" + version + '\'' +
                '}';
    }

}