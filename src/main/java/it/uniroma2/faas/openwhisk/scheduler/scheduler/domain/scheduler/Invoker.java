package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.ContainerAction.DEFAULT_MEMORY_LIMIT_MiB;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler.ContainerAction.getActionIdFrom;
import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * It is supposed that this class is externally synchronized.
 */
public class Invoker {

    public static final long MID_ACTION_CONTAINER_MEMORY_MiB = 64;
    public static final float DEFAULT_ACTION_CONTAINER_LOAD_FACTOR = 0.70f;
    public static final long DEFAULT_ACTIVATION_CONTAINER_CAPACITY = 128;
    public static final float DEFAULT_ACTIVATION_CONTAINER_LOAD_FACTOR = 0.70f;

    public static final int BUFFER_LIMIT = 0;

    // invoker name, corresponding to invoker's activation topic
    private final String invokerName;
    // total memory on invoker node
    private final long userMemory;
    // OPTIMIZE: use ConcurrentHasMap
    // <ActionID, ContainerAction>
    // this variable maps unique string representing an action container with its implementation
    // TODO: to manage stem-cells (pre-warmed containers) modify apache OpenWhisk Controller component
    //   to insert "kind" field inside activation records sent to "scheduler" topic; also it is required
    //   to implement a mechanism to retrieve stem-cells configuration per-runtime, depending on the
    //   deploy of the framework
    private final Map<String, ContainerAction> actionContainerMap;
    // OPTIMIZE: use ConcurrentHashMap
    // <ActivationID, <ContainerAction, timestamp>>
    // maintaining a mapping between activation ids and containers to fast remove activations
    // there is also a timestamp of insertion, that indicate timestamp of successfully acquired concurrency
    private final Map<String, Map.Entry<ContainerAction, Long>> activationContainerMap;
    // buffer limit
    private final int bufferLimit;
    // invoker buffer
    private final Queue<IBufferizable> buffer;

    // available memory
    private long memory;
    // invoker state
    private State state = State.NOT_READY;
    // timestamp of last state update
    private long lastCheck = 0L;

    // invoker's state
    public enum State {
        NOT_READY,
        OFFLINE,
        HEALTHY,
        UNHEALTHY
    }

    public Invoker(@Nonnull String invokerName, long userMemory) {
        this(invokerName, userMemory, BUFFER_LIMIT);
    }

    public Invoker(@Nonnull String invokerName, long userMemory, int bufferLimit) {
        checkNotNull(invokerName, "Invoker name can not be null.");
        checkArgument(userMemory > 0, "User memory must be > 0.");
        checkArgument(bufferLimit >= 0, "Buffer limit must be >= 0.");

        this.invokerName = invokerName;
        this.userMemory = userMemory;
        this.memory = userMemory;
        this.actionContainerMap = new HashMap<>(
                (int) (userMemory / MID_ACTION_CONTAINER_MEMORY_MiB),
                DEFAULT_ACTION_CONTAINER_LOAD_FACTOR
        );
        this.activationContainerMap = new HashMap<>(
                (int) (userMemory / MID_ACTION_CONTAINER_MEMORY_MiB * DEFAULT_ACTION_CONTAINER_LOAD_FACTOR),
                DEFAULT_ACTIVATION_CONTAINER_LOAD_FACTOR
        );
        this.bufferLimit = bufferLimit;
        this.buffer = new ArrayDeque<>(bufferLimit);
    }

    public boolean tryAcquireConcurrency(@Nonnull final IBufferizable bufferizable) {
        checkArgument(activationContainerMap.get(bufferizable.getActivationId()) == null,
                "Activation ID already present in invoker");

        // retrieve action container from action ID
        final ContainerAction containerAction = actionContainerMap.get(getActionIdFrom(bufferizable));
        if (containerAction != null) {
            // check if the container can handle another activation
            if (containerAction.tryAcquireConcurrency()) {
                // insert new activation in current invoker
                activationContainerMap.put(bufferizable.getActivationId(),
                        new AbstractMap.SimpleImmutableEntry<>(containerAction, Instant.now().toEpochMilli()));
                return true;
            }
        }
        // there is no container which can handle current activation
        return false;
    }

    public boolean tryAcquireMemoryAndConcurrency(@Nonnull final IBufferizable bufferizable) {
        // try acquire concurrency first
        if (tryAcquireConcurrency(bufferizable)) return true;

        // if concurrency can not be acquired, try acquire memory
        // if memory can be acquired, acquire also concurrency
        // using default value for bufferizable that does not have
        //   memory limit is safe as long as it will be released in future
        long memory = bufferizable.getMemoryLimit() == null
                ? DEFAULT_MEMORY_LIMIT_MiB
                : bufferizable.getMemoryLimit();
        // if memory is available, try acquiring concurrency resources
        if (this.memory - memory >= 0) {
            // get action id from bufferizable
            final String actionId = getActionIdFrom(bufferizable);
            // retrieve action container from state map
            final ContainerAction containerAction = actionContainerMap.get(actionId);
            // if there is not yet a action container with this actionId, create new one,
            //   acquire concurrency on it and insert in the state map
            if (containerAction == null) {
                actionContainerMap.put(actionId, ContainerAction.from(bufferizable));
            // if there is already a container with that actionId, create new container with the same
            //   actionId, and try acquire concurrency
            } else {
                containerAction.createContainer();
            }
            // decrease memory capacity caused by new container creation
            this.memory -= memory;
            // acquire concurrency on newly created action container
            return tryAcquireConcurrency(bufferizable);
        }

        // if container actions are removed on release phase instantly, the following code
        //   is no more required
        /*// if memory is not available for new container, check if there is
        //   at least one unused container, with concurrency level of 0
        final String containerToRemove = getFirstUnusedContainer(actionContainerMap);
        // if there is at least one (unique) container that can be removed, so do it
        if (containerToRemove != null) {
            // remove selected unused action container and increase invoker's available memory
            this.memory += actionContainerMap.remove(containerToRemove).getMemoryLimit();
            // now retry acquire memory and concurrency
            return tryAcquireMemoryAndConcurrency(bufferizable);
        }*/

        return false;
    }

    public boolean tryBuffering(@Nonnull final IBufferizable bufferizable) {
        if (buffer.size() < bufferLimit) {
            buffer.add(bufferizable);
            return true;
        }
        // no more space in buffer
        return false;
    }

    public boolean tryAcquireFromBuffer() {
        return buffer.removeIf(this::tryAcquireMemoryAndConcurrency);
    }

    public void release(@Nonnull final String activationId) {
        final Map.Entry<ContainerAction, Long> containerActionTimestampEntry =
                activationContainerMap.remove(activationId);
        if (containerActionTimestampEntry != null) {
            final ContainerAction containerAction = containerActionTimestampEntry.getKey();

            /*// leave one container with no concurrency if no resources are required
            final long containersCountBeforeRelease = containerAction.getContainersCount();
            containerAction.release();
            final long containersReleased = containersCountBeforeRelease - containerAction.getContainersCount();
            if (containersReleased > 0) {
                memory += containersReleased * containerAction.getMemoryLimit();
            }*/

            // release and remove container action instantly to free resources
            final long containersCountBeforeRelease = containerAction.getContainersCount();
            containerAction.release();
            long containersReleased = containersCountBeforeRelease - containerAction.getContainersCount();
            if (containerAction.getConcurrency() == 0 && containerAction.getContainersCount() == 1) {
                actionContainerMap.remove(containerAction.getActionId());
                ++containersReleased;
            }
            if (containersReleased > 0) {
                memory += containersReleased * containerAction.getMemoryLimit();
            }
        } else {
            // try to remove activation from buffer, if any,
            //   since it is possible that one activation is received before another,
            //   due to network delay, and the activation that Scheduler thinks is buffered in
            //   invoker might not be and can be executed
            buffer.removeIf(activation -> activation.getActivationId().equals(activationId));
        }
    }

    public void releaseAllOldThan(long delta) {
        final long now = Instant.now().toEpochMilli();
        activationContainerMap.entrySet().stream()
                .filter(entry -> now - entry.getValue().getValue() > delta)
                .map(Map.Entry::getKey)
                .collect(toUnmodifiableList())
                .forEach(this::release);
    }

    /**
     * Return first {@link ContainerAction} with concurrency level equal to 0.
     *
     * @return first {@link ContainerAction} that can be removed.
     */
    private @Nullable String getFirstUnusedContainer(@Nonnull Map<String, ContainerAction> containerActionMap) {
        for (final Map.Entry<String, ContainerAction> containerAction : containerActionMap.entrySet()) {
            // find first unused container action and remove it
            if (containerAction.getValue().getConcurrency() == 0 &&
                    containerAction.getValue().getContainersCount() == 1) {
                return containerAction.getKey();
            }
        }
        return null;
    }

    public String getInvokerName() {
        return invokerName;
    }

    public long getUserMemory() {
        return userMemory;
    }

    public long getMemory() {
        return memory;
    }

    public long getContainersCount() {
        long containersCount = 0;
        for (final ContainerAction containerAction : actionContainerMap.values()) {
            containersCount += containerAction.getContainersCount();
        }
        return containersCount;
    }

    public long getActivationsCount() {
        return activationContainerMap.size();
    }

    public int getBufferLimit() {
        return bufferLimit;
    }

    public int getBufferSize() {
        return buffer.size();
    }

    public void updateState(@Nonnull State state) {
        updateState(state, lastCheck);
    }

    public void updateState(@Nonnull State state, long timestamp) {
        checkArgument(timestamp >= 0 && timestamp >= this.lastCheck,
                "Timestamp must be >= 0 and >= of previous one (previous: {}).", this.lastCheck);
        this.lastCheck = timestamp;
        this.state = state;
    }

    public State getState() {
        return state;
    }

    public boolean isHealthy() {
        return state == State.HEALTHY;
    }

    public long getLastCheck() {
        return lastCheck;
    }

    public void setLastCheck(long lastCheck) {
        checkArgument(lastCheck >= 0 && lastCheck >= this.lastCheck,
                "Timestamp must be >= 0 and >= of previous one (previous: {}).", this.lastCheck);
        this.lastCheck = lastCheck;
    }

    /**
     * Remove all registered activations and all {@link ContainerAction}s.
     * All memory became available.
     */
    public void removeAll() {
        activationContainerMap.clear();
        actionContainerMap.clear();
        buffer.clear();
        memory = userMemory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Invoker invoker = (Invoker) o;
        return userMemory == invoker.userMemory && Objects.equals(invokerName, invoker.invokerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(invokerName, userMemory);
    }

    @Override
    public String toString() {
        return "Invoker{" +
                "invokerName='" + invokerName + '\'' +
                ", userMemory=" + userMemory +
                ", actionContainerMap=" + actionContainerMap +
                ", activationContainerMap=" + activationContainerMap +
                ", memory=" + memory +
                ", state=" + state +
                ", update=" + lastCheck +
                '}';
    }

}