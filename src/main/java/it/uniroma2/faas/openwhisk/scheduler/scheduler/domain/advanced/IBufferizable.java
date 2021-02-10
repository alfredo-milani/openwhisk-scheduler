package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Action;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nullable;

public interface IBufferizable extends ISchedulable {

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
    @Nullable Long getUserMemory();

    /**
     * Return action.
     *
     * @return {@link Action} which will be processed.
     */
    @Nullable Action getAction();

}
