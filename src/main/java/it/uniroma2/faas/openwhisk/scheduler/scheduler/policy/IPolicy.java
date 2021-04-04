package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Queue;

public interface IPolicy {

    int DEFAULT_PRIORITY = 0;

    /**
     * Used to apply implemented poolicy to input {@link ISchedulable} collection.
     *
     * @param schedulables collection to which to apply policy.
     * @return {@link Queue} containing {@link ISchedulable} objects sorted based on implemented policy.
     */
    @Nonnull Queue<? extends ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables);

    /**
     * Update state of policy, if implemented one has one.
     *
     * @param consumables collection of {@link Completion} needed to updated policy' state.
     */
    void update(@Nonnull final Collection<? extends IConsumable> consumables);

    /**
     * Retrieve policy type.
     *
     * @return implemented {@link Policy}.
     */
    @Nonnull Policy getPolicy();

}
