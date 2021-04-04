package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.Completion;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

class PassThroughPolicy implements IPolicy {

    public static final Policy POLICY = Policy.PASS_THROUGH;

    @Override
    public @Nonnull Queue<ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        return new ArrayDeque<>(schedulables);
    }

    @Override
    public void update(@Nonnull final Collection<? extends IConsumable> consumables) {

    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

}