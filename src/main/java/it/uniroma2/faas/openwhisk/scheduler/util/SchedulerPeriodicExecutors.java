package it.uniroma2.faas.openwhisk.scheduler.util;

import javax.annotation.Nullable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SchedulerPeriodicExecutors {

    private static final int NETWORK_IO_THREAD_COUNT = 5;
    private static final int COMPUTATION_THREAD_COUNT = 5;

    private final ScheduledExecutorService networkIO;
    private final ScheduledExecutorService computation;

    // see@ https://stackoverflow.com/questions/12908412/print-hello-world-every-x-seconds/12908477#12908477
    public SchedulerPeriodicExecutors() {
        this(NETWORK_IO_THREAD_COUNT, COMPUTATION_THREAD_COUNT);
    }

    public SchedulerPeriodicExecutors(int networkIOThreadsCount, int computationThreadsCount) {
        this.networkIO = networkIOThreadsCount > 0
                ? Executors.newScheduledThreadPool(networkIOThreadsCount)
                : null;
        this.computation = computationThreadsCount > 0
                ? Executors.newScheduledThreadPool(computationThreadsCount)
                : null;
    }

    public SchedulerPeriodicExecutors(@Nullable ScheduledExecutorService networkIO,
                                      @Nullable ScheduledExecutorService computation) {
        this.networkIO = networkIO;
        this.computation = computation;
    }

    public @Nullable ScheduledExecutorService networkIO() {
        return networkIO;
    }

    public @Nullable ScheduledExecutorService computation() {
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