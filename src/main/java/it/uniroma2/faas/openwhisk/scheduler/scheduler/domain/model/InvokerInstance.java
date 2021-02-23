package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.Nonnull;
import java.util.Objects;

public class InvokerInstance {

    /*
     * {
     *   "instance": 0,
     *   "instanceType": "invoker",
     *   "uniqueName": "owdev-invoker-0",
     *   "userMemory": "2147483648 B"
     * }
     */

    private final int instance;
    private final Type instanceType;
    private final String uniqueName;
    private final String userMemory;

    public enum Type {
        INVOKER,
        CONTROLLER;

        @JsonCreator
        public static @Nonnull Type from(@Nonnull String type) {
            for (Type i : Type.values()) {
                if (type.equalsIgnoreCase(i.name())) {
                    return i;
                }
            }
            throw new TypeNotPresentException(type, new Throwable("Selected type not yet implemented."));
        }

        @JsonValue
        public @Nonnull String getName() {
            return this.name().toLowerCase();
        }
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public InvokerInstance(@JsonProperty("instance") int instance,
                           @JsonProperty("instanceType") Type instanceType,
                           @JsonProperty("uniqueName") String uniqueName,
                           @JsonProperty("userMemory") String userMemory) {
        this.instance = instance;
        this.instanceType = instanceType;
        this.uniqueName = uniqueName;
        this.userMemory = userMemory;
    }

    public int getInstance() {
        return instance;
    }

    public Type getInstanceType() {
        return instanceType;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public String getUserMemory() {
        return userMemory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvokerInstance invokerInstance1 = (InvokerInstance) o;
        return instance == invokerInstance1.instance && instanceType == invokerInstance1.instanceType &&
                Objects.equals(uniqueName, invokerInstance1.uniqueName) && Objects.equals(userMemory, invokerInstance1.userMemory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instance, instanceType, uniqueName, userMemory);
    }

    @Override
    public String toString() {
        return "ComponentInstance{" +
                "instance=" + instance +
                ", instanceType=" + instanceType +
                ", uniqueName='" + uniqueName + '\'' +
                ", userMemory='" + userMemory + '\'' +
                '}';
    }

}