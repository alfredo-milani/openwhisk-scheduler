package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkNotNull;

class PassThroughPolicy implements IPolicy {

    public static final Policy POLICY = Policy.PASS_THROUGH;

    @Override
    public @Nonnull <T extends ISchedulable> Queue<T> apply(@Nonnull final Collection<T> schedulables) {
        checkNotNull(schedulables, "Input collection of schedulables can not be null.");
        return new ArrayDeque<>(schedulables);
    }

    @Override
    public @Nonnull Policy getPolicy() {
        return POLICY;
    }

}