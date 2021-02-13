package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class NonBlockingCompletion extends Completion {

    /* Non-blocking completion
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

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public NonBlockingCompletion(@JsonProperty("activationId") String activationId, @JsonProperty("instance") Instance instance,
                                 @JsonProperty("isSystemError") Boolean systemError, @JsonProperty("transid") TransId transId) {
        super(instance, systemError, transId);
        this.activationId = activationId;
    }

    public String getActivationId() {
        return activationId;
    }

    @Override
    public String toString() {
        return "NonBlockingCompletion{" +
                "activationId='" + activationId + '\'' +
                "} " + super.toString();
    }

}