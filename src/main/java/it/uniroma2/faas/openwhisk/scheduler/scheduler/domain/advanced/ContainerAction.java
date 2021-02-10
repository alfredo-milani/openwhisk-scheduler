package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced;

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

    public void release() {
        if (concurrency > 0) {
            --concurrency;
            containersCount -= (containersCount * concurrencyLimit - concurrency) / concurrencyLimit;
        }
    }

    public void removeContainer() {
        if (containersCount > 1) --containersCount;
    }

    public void createNewContainer() {
        ++containersCount;
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