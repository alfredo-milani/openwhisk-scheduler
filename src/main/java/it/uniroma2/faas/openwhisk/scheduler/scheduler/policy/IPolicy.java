package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Queue;

public interface IPolicy {

    @Nonnull <T extends ISchedulable> Queue<T> apply(@Nonnull final Collection<T> schedulables);

    @Nonnull Policy getPolicy();

}
