package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.scheduler;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Action;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.RootControllerIndex;

import javax.annotation.Nullable;

public interface IBufferizable extends ISchedulable {

    /**
     * Return information about controller which has generated current activation record.
     *
     * @return {@link RootControllerIndex} which has generated current activation record.
     */
    @Nullable RootControllerIndex getRootControllerIndex();

    /**
     * Return information about action runtime.
     *
     * @return string representing action runtime.
     */
    @Nullable String getKind();

    /**
     * Return information about invoker overload.
     *
     * @return true on invoker overload, false otherwise.
     */
    @Nullable Boolean getOverload();

    /**
     * Return concurrency limits of current action.
     *
     * @return an integer representing concurrency limit for current activation.
     */
    @Nullable Long getConcurrencyLimit();

    /**
     * Return memory limit for current invoked action.
     *
     * @return an integer representing memory limit for current invoked action.
     */
    @Nullable Long getMemoryLimit();

    /**
     * Return time limit of current invoked action.
     *
     * @return an integer representing time limit for current invoked action.
     */
    @Nullable Long getTimeLimit();

    /**
     * Return number of total bytes in target invoker.
     *
     * @return a long representing total user memory in the target invoker.
     */
    @Deprecated(since = "0.1.17")
    @Nullable Long getUserMemory();

    /**
     * Return action.
     *
     * @return {@link Action} which will be processed.
     */
    @Nullable Action getAction();

}
