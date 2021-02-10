package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;

import javax.annotation.Nullable;

public interface ITraceable extends ISchedulable {

    /**
     * Cause field identify a composition.
     *
     * @return string representing Primary Activation ID for a composition.
     */
    @Nullable String getCause();

}
