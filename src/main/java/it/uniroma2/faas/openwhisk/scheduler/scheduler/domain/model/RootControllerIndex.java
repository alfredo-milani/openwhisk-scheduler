package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class RootControllerIndex {

    /*
     * {
     *   "rootControllerIndex": {
     *     "asString": "0",
     *     "instanceType": "controller"
     *   }
     * }
     */

    private final String asString;
    private final String instanceType;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RootControllerIndex(@JsonProperty("asString") String asString,
                               @JsonProperty("instanceType") String instanceType) {
        this.asString = asString;
        this.instanceType = instanceType;
    }

    public String getAsString() {
        return asString;
    }

    public String getInstanceType() {
        return instanceType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RootControllerIndex that = (RootControllerIndex) o;
        return Objects.equals(asString, that.asString) && Objects.equals(instanceType, that.instanceType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asString, instanceType);
    }

    @Override
    public String toString() {
        return "RootControllerIndex{" +
                "asString='" + asString + '\'' +
                ", instanceType='" + instanceType + '\'' +
                '}';
    }

}