package it.uniroma2.faas.openwhisk.scheduler.data.source;

import it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model.IConsumable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Contract between a {@link ISubject} and an {@link IObserver} where the subject can notify data of certain type.
 * Using wildcard ensure that subject implementation is not bounded to a specific type of {@link IConsumable}.
 */
public interface ISubject {

    void register(@Nonnull List<IObserver> observers);
    void unregister(@Nonnull List<IObserver> observers);

    void update();
    boolean hasUpdates();
    // <T> void notifyObservers(@Nonnull final Map<String, Collection<T>> streamToData);
    // <T> void notifyObservers(@Nonnull final Map<? extends IStream, Collection<T>> streamToData);
    <T> void notifyObservers(@Nonnull final Map<UUID, Collection<T>> streamToData);
    // OPTIMIZE: signature should be as the following?
    // <T> void notifyObservers(@Nonnull final Map<UUID, Collection<? extends T>> streamToData);

    // @Deprecated(since = "0.1.0")
    // @Nonnull <T> Map<String, Collection<T>> streamsPartition(@Nonnull final Collection<T> data);
    // interface IStream {}
    // @Nonnull <T> Map<? extends IStream, Collection<T>> streamsPartition(@Nonnull final Collection<T> data);
    @Nonnull <T> Map<UUID, Collection<T>> streamsPartition(@Nonnull final Collection<T> data);

}
