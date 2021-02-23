package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class FailureCompletion extends Completion {

    /* In case of error
     * {
     *   "instance": {
     *     "instance": 0,
     *     "instanceType": "invoker",
     *     "uniqueName": "owdev-invoker-0",
     *     "userMemory": "2147483648 B"
     *   },
     *   "isSystemError": true,
     *   "response": "9376ac0a2ed848abb6ac0a2ed818ab0b",
     *   "transid": [
     *     "VqtARfumgJ3EcWpjjUHRm8zGytUKseup",
     *     1613235607414
     *   ]
     * }
     */

    private final String response;

    public FailureCompletion(@JsonProperty("instance") InvokerInstance invokerInstance, @JsonProperty("isSystemError") Boolean systemError,
                             @JsonProperty("transid") TransId transId, @JsonProperty("response") String response) {
        super(invokerInstance, systemError, transId);
        this.response = response;
    }

    public String getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "FailureCompletion{" +
                "response='" + response + '\'' +
                "} " + super.toString();
    }

}