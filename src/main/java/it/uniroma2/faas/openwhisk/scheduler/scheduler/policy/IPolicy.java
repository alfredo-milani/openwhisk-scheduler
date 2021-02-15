package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Queue;

public interface IPolicy {

    int DEFAULT_PRIORITY = 0;

    @Nonnull Queue<? extends ISchedulable> apply(@Nonnull final Collection<? extends ISchedulable> schedulables);

    @Nonnull Policy getPolicy();

}
