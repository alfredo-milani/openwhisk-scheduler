package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Completion implements IConsumable {

    private final InvokerInstance invokerInstance;
    private final Boolean systemError;
    private final TransId transId;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Completion(@JsonProperty("instance") InvokerInstance invokerInstance, @JsonProperty("isSystemError") Boolean systemError,
                      @JsonProperty("transid") TransId transId) {
        this.invokerInstance = invokerInstance;
        this.systemError = systemError;
        this.transId = transId;
    }

    public InvokerInstance getInstance() {
        return invokerInstance;
    }

    public Boolean getSystemError() {
        return systemError;
    }

    public TransId getTransId() {
        return transId;
    }

    @Override
    public String toString() {
        return "Completion{" +
                ", instance=" + invokerInstance +
                ", systemError=" + systemError +
                ", transId=" + transId +
                '}';
    }

}