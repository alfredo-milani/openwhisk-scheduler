package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class BlockingCompletion extends Completion {

    /* Blocking request
     * {
     *   "instance": {
     *     "instance": 0,
     *     "instanceType": "invoker",
     *     "uniqueName": "owdev-invoker-0",
     *     "userMemory": "2147483648 B"
     *   },
     *   "isSystemError": false,
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
     *   },
     *   "transid": [
     *     "iud25hHHjA0mtOTri9nKePhEnDhcMpH2",
     *     1612889790960
     *   ]
     * }
     */

    /*
     * {
     *   "instance": {
     *     "instance": 0,
     *     "instanceType": "invoker",
     *     "uniqueName": "owdev-invoker-0",
     *     "userMemory": "2147483648 B"
     *   },
     *   "isSystemError": false,
     *   "response": {
     *     "activationId": "81785d241fe94fd2b85d241fe9bfd24e",
     *     "annotations": [
     *       {
     *         "key": "causedBy",
     *         "value": "sequence"
     *       },
     *       {
     *         "key": "path",
     *         "value": "guest/cmp"
     *       },
     *       {
     *         "key": "waitTime",
     *         "value": 960
     *       },
     *       {
     *         "key": "kind",
     *         "value": "nodejs:10"
     *       },
     *       {
     *         "key": "timeout",
     *         "value": false
     *       },
     *       {
     *         "key": "limits",
     *         "value": {
     *           "concurrency": 3,
     *           "logs": 10,
     *           "memory": 256,
     *           "timeout": 60000
     *         }
     *       },
     *       {
     *         "key": "initTime",
     *         "value": 530
     *       }
     *     ],
     *     "cause": "802c89ce6b414636ac89ce6b4196361f",
     *     "duration": 613,
     *     "end": 1613386872211,
     *     "logs": [],
     *     "name": "cmp",
     *     "namespace": "guest",
     *     "publish": false,
     *     "response": {
     *       "result": {
     *         "action": "/_/fn1",
     *         "method": "action",
     *         "params": {
     *           "$scheduler": {
     *             "duration": 7,
     *             "limits": {
     *               "concurrency": 3,
     *               "memory": 256,
     *               "timeout": 60000,
     *               "userMemory": 2048
     *             },
     *             "overload": false,
     *             "priority": 0,
     *             "target": "invoker0"
     *           },
     *           "sleep_time": 5,
     *           "user": "Kira"
     *         },
     *         "state": {
     *           "$composer": {
     *             "resuming": true,
     *             "session": "81785d241fe94fd2b85d241fe9bfd24e",
     *             "stack": [],
     *             "state": 2
     *           }
     *         }
     *       },
     *       "size": 335,
     *       "statusCode": 0
     *     },
     *     "start": 1613386871598,
     *     "subject": "guest",
     *     "version": "0.0.2"
     *   },
     *   "transid": [
     *     "Bh2gbKpjKcjV0Jj1GSlvv8cxQ8berti7",
     *     1613386870556,
     *     [
     *       "G6yqpDPu9WsjFdpZxIIS1CEe4sRzovMY",
     *       1613386870514
     *     ]
     *   ]
     * }
     */

    private final Response response;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BlockingCompletion(@JsonProperty("instance") InvokerInstance invokerInstance, @JsonProperty("isSystemError") Boolean systemError,
                              @JsonProperty("response") Response response, @JsonProperty("transid") TransId transId) {
        super(invokerInstance, systemError, transId);
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "BlockingCompletion{" +
                "response=" + response +
                "} " + super.toString();
    }

}