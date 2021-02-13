package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Action;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerAction extends Action {

    public static final long DEFAULT_CONCURRENCY_LIMIT = 1;
    public static final long DEFAULT_MEMORY_LIMIT_MiB = 256;
    public static final long DEFAULT_TIME_LIMIT_MS = TimeUnit.SECONDS.toMillis(60);

    // public static final long DEFAULT_ACTIVATIONS_SET_CAPACITY = 256;
    // public static final float DEFAULT_ACTIVATIONS_SET_LOAD_FACTOR = 70.0f;

    private static final String ACTION_ID_TEMPLATE = "%s/%s/%s";

    private final long concurrencyLimit;
    private final long memoryLimit;
    private final long timeLimit;

    /*private final Set<String> activations = new HashSet<>(
            (int) DEFAULT_ACTIVATIONS_SET_CAPACITY,
            DEFAULT_ACTIVATIONS_SET_LOAD_FACTOR
    );*/
    private long concurrency = 0;
    private long containersCount = 1;

    public ContainerAction(String name, String path, String version,
                           long concurrencyLimit, long memoryLimit, long timeLimit) {
        super(name, path, version);
        this.concurrencyLimit = concurrencyLimit;
        this.memoryLimit = memoryLimit;
        this.timeLimit = timeLimit;
    }

    /**
     * Create a {@link ContainerAction} from {@link IBufferizable} allowing using default values
     * if concurrency limit, memory limit or time limit are not specified in {@link IBufferizable}
     * object. Using default settings is not a problem because a {@link ContainerAction} is automatically
     * destroyed
     *
     * @param bufferizable object from which to obtain information to create a {@link ContainerAction}.
     * @return a {@link ContainerAction} defined from {@link IBufferizable}.
     */
    public static @Nonnull ContainerAction from(@Nonnull IBufferizable bufferizable) {
        checkNotNull(bufferizable, "Activation can not be null.");
        checkArgument(bufferizable.getAction() != null, "Action can not be null.");

        final Action action = bufferizable.getAction();
        long concurrencyLimit = bufferizable.getConcurrencyLimit() == null
                ? DEFAULT_CONCURRENCY_LIMIT
                : bufferizable.getConcurrencyLimit();
        long memoryLimit = bufferizable.getMemoryLimit() == null
                ? DEFAULT_MEMORY_LIMIT_MiB
                : bufferizable.getMemoryLimit();
        long timeLimit = bufferizable.getTimeLimit() == null
                ? DEFAULT_TIME_LIMIT_MS
                : bufferizable.getTimeLimit();
        return new ContainerAction(
                action.getName(), action.getPath(), action.getVersion(),
                concurrencyLimit,
                memoryLimit,
                timeLimit
        );
    }

    /*public boolean tryAcquireConcurrency(@Nonnull String activation) {
        checkNotNull(activation, "Activation can not be null.");
        if (activations.size() < concurrencyLimit * containersCount) {
            activations.add(activation);
            return true;
        }
        return false;
    }*/

    /**
     * Acquire at most one concurrency slot.
     *
     * @return true if the concurrency slot has been acquired, false otherwise.
     */
    public boolean tryAcquireConcurrency() {
        if (concurrency < concurrencyLimit * containersCount) {
            ++concurrency;
            return true;
        }
        return false;
    }

    /*public void release(@Nonnull String activation) {
        checkNotNull(activation, "Activation can not be null.");
        if (activations.remove(activation)) {
            containersCount -= (containersCount * concurrencyLimit - activations.size()) / concurrencyLimit;
        }
    }*/

    /**
     * Release at most one concurrency slot and remove unused containers if any.
     * Can be removed at most maxContainer - 1.
     */
    public void release() {
        if (concurrency > 0) {
            --concurrency;
            // remove all unused containers, that is containers with concurrency level of 0
            removeContainers((containersCount * concurrencyLimit - concurrency) / concurrencyLimit);
        }
    }

    public void removeContainer() {
        removeContainers(1);
    }

    /**
     * Remove containers from current state.
     * Can be removed at most maxContainer - 1.
     *
     * @param containers number of containers to remove.
     */
    public void removeContainers(long containers) {
        checkArgument(containers > 0, "Numbers of containers to remove must be > 0.");
        // must remain at least one container running
        if (containers >= containersCount) containersCount = 1;
        else containersCount -= containers;
    }

    public void createContainer() {
        createContainers(1);
    }

    public void createContainers(long containers) {
        checkArgument(containers > 0, "Numbers of containers to create must be > 0.");
        containersCount += containers;
    }

    public String getActionId() {
        return String.format(ACTION_ID_TEMPLATE, getPath(), getName(), getVersion());
    }

    public long getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    public long getTimeLimit() {
        return timeLimit;
    }

    public long getConcurrency() {
        return concurrency;
    }

    public long getContainersCount() {
        return containersCount;
    }

    @Override
    public String toString() {
        return "ContainerAction{" +
                "concurrencyLimit=" + concurrencyLimit +
                ", memoryLimit=" + memoryLimit +
                ", timeLimit=" + timeLimit +
                ", concurrency=" + concurrency +
                ", containersCount=" + containersCount +
                '}';
    }

}