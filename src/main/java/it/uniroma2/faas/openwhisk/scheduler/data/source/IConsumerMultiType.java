package it.uniroma2.faas.openwhisk.scheduler.data.source;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Contract to read records from data source.
 */
public interface IConsumerMultiType {

    @Nonnull Collection<? extends IConsumable> consume() throws Exception;

}
