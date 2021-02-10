package it.uniroma2.faas.openwhisk.scheduler.data.source;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.IConsumable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Contract between a subject, which generates collection of type {@link IConsumable}, and an observer.
 * The observer can receive different data of type {@link IConsumable} from different {@link ISubject}.
 * Using wildcard ensure that observer implementation is not bounded to a specific type of {@link IConsumable}.
 */
public interface IObserver {

    void register(@Nonnull List<ISubject> subjects);

    void unregister(@Nonnull List<ISubject> subjects);

//    @Deprecated(since = "0.1.0")
//    void newEvent(@Nonnull String stream, @Nonnull final Collection<?> data);
//    void newEvent(@Nonnull ISubject.IStream stream, @Nonnull final Collection<?> data);
    void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data);

}
