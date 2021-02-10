package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Queue;

class RunningCompositionFIFOPolicy implements IPolicy {

    public static final Policy POLICY = Policy.RUNNING_COMPOSITION_FIFO;

    public RunningCompositionFIFOPolicy() {
        throw new RuntimeException("Policy " + POLICY + " not yet implemented.");
    }

    @Override
    public @Nonnull Queue<ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables) {
        throw new RuntimeException("Policy " + POLICY + " not yet implemented.");
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

}