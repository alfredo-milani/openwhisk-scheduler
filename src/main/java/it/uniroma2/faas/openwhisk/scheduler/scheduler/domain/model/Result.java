package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Result {

    /*
    {
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

    private final Map<String, Object> result;
    private final Long size;
    private final Integer statusCode;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Result(@JsonProperty("result") Map<String, Object> result, @JsonProperty("size") Long size,
                  @JsonProperty("statusCode") Integer statusCode) {
        this.result = result;
        this.size = size;
        this.statusCode = statusCode;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public Long getSize() {
        return size;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    @Override
    public String toString() {
        return "Result{" +
                "result=" + result +
                ", size=" + size +
                ", statusCode=" + statusCode +
                '}';
    }

}