package it.uniroma2.faas.openwhisk.scheduler.data.source;

import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.IConsumable;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;

public interface IProducer extends Closeable {

    <T extends IConsumable> void produce(@Nonnull final String topic, @Nonnull final T data);

    <T extends IConsumable> void produce(@Nonnull final String topic, @Nonnull final Collection<T> data);

}
