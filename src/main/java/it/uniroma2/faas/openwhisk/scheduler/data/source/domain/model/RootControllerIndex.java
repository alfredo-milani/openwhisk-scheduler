package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.Nonnull;

public class RootControllerIndex {

    /*
    "rootControllerIndex":{
          "asString":"0",
          "instanceType":"controller"
       }
     */

    private final String asString;
    private final InstanceType instanceType;

    enum InstanceType {
        CONTROLLER,
        INVOKER;

        @JsonCreator
        public static @Nonnull InstanceType from(@Nonnull String instanceType) {
            for (InstanceType i : InstanceType.values()) {
                if (instanceType.equalsIgnoreCase(i.name())) {
                    return i;
                }
            }
            throw new TypeNotPresentException(instanceType, new Throwable("Selected instanceType not yet implemented"));
        }

        @JsonValue
        public @Nonnull String getName() {
            return this.name().toLowerCase();
        }

    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RootControllerIndex(@JsonProperty("asString") String asString,
                               @JsonProperty("instanceType") InstanceType instanceType) {
        this.asString = asString;
        this.instanceType = instanceType;
    }

    public String getAsString() {
        return asString;
    }

    public InstanceType getInstanceType() {
        return instanceType;
    }

    @Override
    public String toString() {
        return "RootControllerIndex{" +
                "asString='" + asString + '\'' +
                ", instanceType=" + instanceType +
                '}';
    }

}