package it.uniroma2.faas.openwhisk.scheduler.util;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SchedulerExecutors {

    private static final int NETWORK_IO_THREAD_COUNT = 5;
    private static final int COMPUTATION_THREAD_COUNT = 5;

    private final ExecutorService networkIO;
    private final ExecutorService computation;

    // TODO: if Future are necessary see@ https://www.baeldung.com/java-executor-service-tutorial
    public SchedulerExecutors() {
        this(NETWORK_IO_THREAD_COUNT, COMPUTATION_THREAD_COUNT);
    }

    public SchedulerExecutors(int networkIOThreadsCount, int computationThreadsCount) {
        this.networkIO = networkIOThreadsCount > 0
                ? Executors.newFixedThreadPool(networkIOThreadsCount)
                : null;
        this.computation = computationThreadsCount > 0
                ? Executors.newFixedThreadPool(computationThreadsCount)
                : null;
    }

    public SchedulerExecutors(@Nullable ExecutorService networkIO,
                              @Nullable ExecutorService computation) {
        this.networkIO = networkIO;
        this.computation = computation;
    }

    public @Nullable ExecutorService networkIO() {
        return networkIO;
    }

    public @Nullable ExecutorService computation() {
        return computation;
    }

    public void shutdown() {
        if (networkIO != null) {
            networkIO.shutdown();
            try {
                if (!networkIO.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    networkIO.shutdownNow();
                }
            } catch (InterruptedException e) {
                networkIO.shutdownNow();
            }
        }

        if (computation != null) {
            computation.shutdown();
            try {
                if (!computation.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    computation.shutdownNow();
                }
            } catch (InterruptedException e) {
                computation.shutdownNow();
            }
        }
    }

}
