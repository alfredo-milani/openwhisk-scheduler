package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Queue;

class RunningCompositionFIFOPolicy implements IPolicy {

    public static final Policy POLICY = Policy.RUNNING_COMPOSITION_FIFO;

    @Override
    public @Nonnull <T extends ISchedulable> Queue<T> apply(@Nonnull final Collection<T> schedulables) {
        throw new RuntimeException("Policy " + POLICY + " not yet implemented.");
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

}