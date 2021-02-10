package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Completion implements IConsumable {

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

    /* Non-blocking request
     * {
     *   "activationId": "811bd520fa794dbe9bd520fa793dbef3",
     *   "instance": {
     *     "instance": 0,
     *     "instanceType": "invoker",
     *     "uniqueName": "owdev-invoker-0",
     *     "userMemory": "2147483648 B"
     *   },
     *   "isSystemError": false,
     *   "transid": [
     *     "MSCztINkcSGlhgeQT6H7YJAPaDVNO0nK",
     *     1612935312054
     *   ]
     * }
     */

    private final String activationId;
    private final Instance instance;
    private final Boolean systemError;
    private final Response response;
    private final TransId transId;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Completion(@JsonProperty("activationId") String activationId, @JsonProperty("instance") Instance instance,
                      @JsonProperty("isSystemError") Boolean systemError, @JsonProperty("response") Response response,
                      @JsonProperty("transid") TransId transId) {
        this.activationId = activationId;
        this.instance = instance;
        this.systemError = systemError;
        this.response = response;
        this.transId = transId;
    }

    /**
     * Field non null iif the request was non-blocking.
     * @return activation id of the request.
     */
    public String getActivationId() {
        return activationId;
    }

    public Instance getInstance() {
        return instance;
    }

    public Boolean getSystemError() {
        return systemError;
    }

    /**
     * Field non null iif the request was blocking.
     * @return the response of the execution of the action.
     */
    public Response getResponse() {
        return response;
    }

    public TransId getTransId() {
        return transId;
    }

    @Override
    public String toString() {
        return "Completion{" +
                "activationId='" + activationId + '\'' +
                ", instance=" + instance +
                ", systemError=" + systemError +
                ", response=" + response +
                ", transId=" + transId +
                '}';
    }

}