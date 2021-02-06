package it.uniroma2.faas.openwhisk.scheduler.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AppExecutors {

    private static final int NETWORK_IO_THREAD_COUNT = 5;
    private static final int COMPUTATION_THREAD_COUNT = 5;

    private final ExecutorService networkIO;
    private final ExecutorService computation;

    // TODO: vedi se utilizzare ExecutorService per poter utilizzare Future
    //          see@ https://www.baeldung.com/java-executor-service-tutorial
    public AppExecutors() {
        this(
                Executors.newFixedThreadPool(NETWORK_IO_THREAD_COUNT),
                Executors.newFixedThreadPool(COMPUTATION_THREAD_COUNT)
        );
    }

    public AppExecutors(ExecutorService networkIO, ExecutorService computation) {
        this.networkIO = networkIO;
        this.computation = computation;
    }

    public ExecutorService networkIO() {
        return networkIO;
    }

    public ExecutorService computation() {
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
