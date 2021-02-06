package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Completion implements IConsumable {

    /*
    {
  "instance": {
    "instance": 0,
    "instanceType": "invoker",
    "uniqueName": "owdev-invoker-0",
    "userMemory": "2147483648 B"
  },
  "isSystemError": false,
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
    },
    "start": 1612222406340,
    "subject": "guest",
    "version": "0.0.2"
  },
  "transid": [
    "2hQMDKaieAYzeka6BfnoTNiuoszwM9rq",
    1612222405924
  ]
}
     */

    private final Instance instance;
    private final Boolean systemError;
    private final Response response;
    private final TransId transId;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Completion(@JsonProperty("instance") Instance instance, @JsonProperty("isSystemError") Boolean systemError,
                      @JsonProperty("response") Response response, @JsonProperty("transid") TransId transId) {
        this.instance = instance;
        this.systemError = systemError;
        this.response = response;
        this.transId = transId;
    }

    public Instance getInstance() {
        return instance;
    }

    public Boolean getSystemError() {
        return systemError;
    }

    public Response getResponse() {
        return response;
    }

    public TransId getTransId() {
        return transId;
    }

    @Override
    public String toString() {
        return "Completion{" +
                "instance=" + instance +
                ", systemError=" + systemError +
                ", response=" + response +
                ", transId=" + transId +
                '}';
    }

}