package it.uniroma2.faas.openwhisk.scheduler.data.source;

import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.IConsumable;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Contract which provide service to read from a data source and parse records
 * to a single type of {@link IConsumable} object.
 *
 * @param <T> single object of type {@link IConsumable} read from data source.
 */
public interface IConsumer<T extends IConsumable> extends Callable<String>, Closeable {

    /**
     * Consume records from data source and parse them to {@link IConsumable} objects.
     *
     * @param <T> object of type {@link IConsumable} read from data source.
     * @return collection of {@link IConsumable}s.
     * @throws Exception if can not consume from data source or while parsing object.
     */
    @Nullable Collection<T> consume() throws Exception;

}
