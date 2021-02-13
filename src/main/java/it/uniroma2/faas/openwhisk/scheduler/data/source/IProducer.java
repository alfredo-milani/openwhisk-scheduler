package it.uniroma2.faas.openwhisk.scheduler.data.source;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;

public interface IProducer extends Closeable {

    void produce(@Nonnull final String topic, @Nonnull final IConsumable data);

    void produce(@Nonnull final String topic, @Nonnull final Collection<? extends IConsumable> data);

}
