package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Health implements IConsumable {

    /*
     * {
     *   "name": {
     *     "instance": 0,
     *     "instanceType": "invoker",
     *     "uniqueName": "owdev-invoker-0",
     *     "userMemory": "2147483648 B"
     *   }
     * }
     */

    private final InvokerInstance invokerInstance;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Health(@JsonProperty("name") InvokerInstance invokerInstance) {
        this.invokerInstance = invokerInstance;
    }

    public InvokerInstance getInstance() {
        return invokerInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Health health = (Health) o;
        return Objects.equals(invokerInstance, health.invokerInstance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(invokerInstance);
    }

    @Override
    public String toString() {
        return "Health{" +
                "componentInstance=" + invokerInstance +
                '}';
    }

}