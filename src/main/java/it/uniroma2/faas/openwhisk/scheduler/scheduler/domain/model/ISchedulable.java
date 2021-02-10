package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Object that can be scheduled using a {@link IPolicy}.
 * All schedulable objects can be send to an invoker to be processed.
 */
public interface ISchedulable extends IConsumable {

    // to change jackson getter method, see@ https://www.baeldung.com/jackson-annotations#2-jsongetter

    /**
     * Retrieve {@link ISchedulable} ID.
     *
     * @return string representing ID.
     */
    @Nonnull String getActivationId();

    /**
     * Retrieve {@link ISchedulable} priority level.
     *
     * @return integer representing priority level.
     */
    @Nullable Integer getPriority();

    /**
     * Retrieve {@link ISchedulable} target invoker.
     * All {@link ISchedulable} objects are processed to be sent to an invoker, for execution.
     * Nota that, if target invoker is null, the activation should be sent to all invokers.
     *
     * @return string representing target destination.
     */
    @Nullable String getTargetInvoker();

    /**
     * Create new {@link ISchedulable} object with new provided priority level.
     *
     * @param newPriority new priority level selected for new {@link ISchedulable} object.
     * @return new {@link ISchedulable} object with selected priority level.
     */
    @Nonnull <T extends ISchedulable> T with(int newPriority);

}
