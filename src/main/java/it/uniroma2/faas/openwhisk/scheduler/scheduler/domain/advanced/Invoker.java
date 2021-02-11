package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Action;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced.ContainerAction.DEFAULT_MEMORY_LIMIT_MiB;

/**
 * It is supposed that this class is externally synchronized.
 */
public class Invoker {

    public static final long MID_ACTION_CONTAINER_MEMORY_MiB = 64;
    public static final float DEFAULT_ACTION_CONTAINER_LOAD_FACTOR = 0.70f;
    public static final long DEFAULT_ACTIVATION_CONTAINER_CAPACITY = 128;
    public static final float DEFAULT_ACTIVATION_CONTAINER_LOAD_FACTOR = 0.70f;

    private static final String ACTION_ID_TEMPLATE = "%s/%s/%s";

    // invoker name
    private final String invokerName;
    // total memory on invoker node
    private final long userMemory;
    // OPTIMIZE: use ConcurrentHasMap
    // <ActionID, ContainerAction>
    // this variable maps unique string representing an action container with its implementation
    private final Map<String, ContainerAction> actionContainerMap;
    // OPTIMIZE: use ConcurrentHashMap
    // <ActivationID, ContainerAction>
    // maintaining a mapping between activation ids and containers to fast remove activations
    private final Map<String, ContainerAction> activationContainerMap;

    // available memory
    private long memory;

    public Invoker(@Nonnull String invokerName, long userMemory) {
        checkNotNull(invokerName, "Invoker name can not be null.");
        checkArgument(userMemory > 0, "User memory must be > 0.");

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
    }

    public boolean tryAcquireConcurrency(@Nonnull final IBufferizable bufferizable) {
        checkNotNull(bufferizable, "Activation can not be null.");
        checkArgument(activationContainerMap.get(bufferizable.getActivationId()) == null,
                "Activation ID already present in invoker");

        // retrieve action container from action ID
        final ContainerAction containerAction = actionContainerMap.get(getActionIdFrom(bufferizable));
        if (containerAction != null) {
            // check if the container can handle another activation
            if (containerAction.tryAcquireConcurrency()) {
                activationContainerMap.put(bufferizable.getActivationId(), containerAction);
                return true;
            }
        }
        // there is no container which can handle current activation
        return false;
    }

    public boolean tryAcquireMemoryAndConcurrency(@Nonnull final IBufferizable bufferizable) {
        checkNotNull(bufferizable, "Activation can not be null.");

        // try acquire concurrency first
        if (tryAcquireConcurrency(bufferizable)) return true;

        // if concurrency can not be acquired, try acquire memory
        // if memory can be acquired, acquire also concurrency
        // using default value for bufferizable which does not have
        //   memory limit is safe as long as it will be released in future
        long memory = bufferizable.getMemoryLimit() == null
                ? DEFAULT_MEMORY_LIMIT_MiB
                : bufferizable.getMemoryLimit();
        // if memory is available, try acquiring concurrency resources
        if (this.memory - memory >= 0) {
            createContainerFor(bufferizable, actionContainerMap);
            // decrease memory capacity caused by new container creation
            this.memory -= memory;
            // acquire concurrency on newly created action container
            return tryAcquireConcurrency(bufferizable);
        }

        // if memory is not available for new container, check if there is
        //   at least one unused container, with concurrency level of 0
        final String containerToRemove = getFirstUnusedContainer(actionContainerMap);
        // if there is at least one (unique) container that can be removed, so do it
        if (containerToRemove != null) {
            // remove selected unused action container and increase invoker's available memory
            this.memory += actionContainerMap.remove(containerToRemove).getMemoryLimit();
            // now try acquire memory and concurrency
            return tryAcquireMemoryAndConcurrency(bufferizable);
        }

        return false;
    }

    public void release(@Nonnull final String activationId) {
        checkNotNull(activationId, "Activation ID can not be null.");

        final ContainerAction containerAction = activationContainerMap.remove(activationId);
        if (containerAction != null) {
            final long containersCountBeforeRelease = containerAction.getContainersCount();
            containerAction.release();
            final long containersReleased = containersCountBeforeRelease - containerAction.getContainersCount();
            if (containersReleased > 0) {
                memory += containersReleased * containerAction.getMemoryLimit();
            }
        }
    }

    /**
     * Create new {@link ContainerAction} from specified {@link IBufferizable} and add it to
     * specified map.
     *
     * @param bufferizable from which create new {@link ContainerAction}.
     * @param containerActionMap to create.
     */
    private void createContainerFor(@Nonnull IBufferizable bufferizable,
                                    @Nonnull Map<String, ContainerAction> containerActionMap) {
        checkNotNull(bufferizable, "Bufferizable can not be null.");
        checkNotNull(containerActionMap, "Container action map can not be null.");

        // get action id from bufferizable
        final String actionId = getActionIdFrom(bufferizable);
        // retrieve action container from state map
        final ContainerAction containerAction = containerActionMap.get(actionId);
        // if there is not yet a action container with this actionId, create new one,
        //   acquire concurrency on it and insert in the state map
        if (containerAction == null) {
            containerActionMap.put(actionId, ContainerAction.from(bufferizable));
            // if there is already a container with that actionId, create new container with the same
            //   actionId, and try acquire concurrency
        } else {
            containerAction.createContainer();
        }
    }

    /**
     * Return first {@link ContainerAction} with concurrency level equal to 0.
     *
     * @return first {@link ContainerAction} that can be removed.
     */
    private @Nullable String getFirstUnusedContainer(@Nonnull Map<String, ContainerAction> containerActionMap) {
        checkNotNull(containerActionMap, "Container action map can not be null.");

        for (final Map.Entry<String, ContainerAction> containerAction : containerActionMap.entrySet()) {
            // find first unused container action and remove it
            if (containerAction.getValue().getConcurrency() == 0 &&
                    containerAction.getValue().getContainersCount() == 1) {
                return containerAction.getKey();
            }
        }
        return null;
    }

    public static @Nonnull String getActionIdFrom(@Nonnull final IBufferizable bufferizable) {
        checkNotNull(bufferizable, "Activation can not be null.");
        checkArgument(bufferizable.getAction() != null, "Action can not be null.");
        final Action action = bufferizable.getAction();
        return String.format(ACTION_ID_TEMPLATE, action.getPath(), action.getName(), action.getVersion());
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

    @Override
    public String toString() {
        return "Invoker{" +
                "invokerName='" + invokerName + '\'' +
                ", userMemory=" + userMemory +
                ", actionContainerMap=" + actionContainerMap +
                ", memory=" + memory +
                '}';
    }

}