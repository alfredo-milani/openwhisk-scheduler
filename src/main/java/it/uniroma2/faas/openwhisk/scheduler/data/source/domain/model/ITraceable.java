package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import javax.annotation.Nullable;

public interface ITraceable extends ISchedulable {

    /**
     * Cause field identify a composition.
     *
     * @return string representing Primary Activation ID for a composition.
     */
    @Nullable String getCause();

}
