package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Completion implements IConsumable {

    private final Instance instance;
    private final Boolean systemError;
    private final TransId transId;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Completion(@JsonProperty("instance") Instance instance, @JsonProperty("isSystemError") Boolean systemError,
                      @JsonProperty("transid") TransId transId) {
        this.instance = instance;
        this.systemError = systemError;
        this.transId = transId;
    }

    public Instance getInstance() {
        return instance;
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
                ", instance=" + instance +
                ", systemError=" + systemError +
                ", transId=" + transId +
                '}';
    }

}