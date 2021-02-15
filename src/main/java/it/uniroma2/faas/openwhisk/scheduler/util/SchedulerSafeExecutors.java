package it.uniroma2.faas.openwhisk.scheduler.util;

import javax.annotation.Nullable;
import java.util.concurrent.*;

public class SchedulerSafeExecutors {

    private static final int NETWORK_IO_THREAD_COUNT = 5;
    private static final int COMPUTATION_THREAD_COUNT = 5;

    private final ExecutorService networkIO;
    private final ExecutorService computation;

    private static final class SafeThreadPoolExecutor extends ThreadPoolExecutor {

        public SafeThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                      long keepAliveTime, TimeUnit unit,
                                      BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);

            // if submit() method is called instead of execute()
            if (t == null && r instanceof  Future<?>) {
                try {
                    Object result = ((Future<?>) r).get();
                } catch (CancellationException e) {
                    t = e;
                } catch (ExecutionException e) {
                    t = e.getCause();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // exception occurred
            if (t != null) {
                /*System.err.println("Uncaught exception is detected! " + t
                        + " st: " + Arrays.toString(t.getStackTrace()));*/
                // handling exception restarting the runnable again
                execute(r);
            }
        }

    }

    // see@ https://aozturk.medium.com/how-to-handle-uncaught-exceptions-in-java-abf819347906
    public SchedulerSafeExecutors() {
        this(NETWORK_IO_THREAD_COUNT, COMPUTATION_THREAD_COUNT);
    }

    public SchedulerSafeExecutors(int networkIOThreadsCount, int computationThreadsCount) {
        this.networkIO = networkIOThreadsCount > 0
                ? new SafeThreadPoolExecutor(networkIOThreadsCount, networkIOThreadsCount,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>())
                : null;
        this.computation = computationThreadsCount > 0
                ? new SafeThreadPoolExecutor(computationThreadsCount, computationThreadsCount,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>())
                : null;
    }

    public SchedulerSafeExecutors(@Nullable ExecutorService networkIO,
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