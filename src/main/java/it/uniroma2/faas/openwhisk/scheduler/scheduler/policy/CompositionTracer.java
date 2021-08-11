package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Queue;

// TODO: Delete Advanced Scheduler TracerScheduler and implements it as policy instead
public class CompositionTracer implements IPolicy {

    public static final Policy POLICY = Policy.COMPOSITION_TRACER;

    @Override
    public @Nonnull Queue<? extends ISchedulable> apply(@Nonnull Collection<? extends ISchedulable> schedulables) {
        return null;
    }

    @Override
    public Queue<? extends IConsumable> update(@Nonnull Collection<? extends IConsumable> consumables) {
        return null;
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

}