package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Health implements IConsumable {

    /*
    {"name":{"instance":0,"instanceType":"invoker","uniqueName":"owdev-invoker-0","userMemory":"2147483648 B"}}
     */

    private final Instance instance;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Health(@JsonProperty("name") Instance instance) {
        this.instance = instance;
    }

    public Instance getComponentInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return "Health{" +
                "componentInstance=" + instance +
                '}';
    }

}