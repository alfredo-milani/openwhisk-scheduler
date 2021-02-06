package it.uniroma2.faas.openwhisk.scheduler.scheduler;

import it.uniroma2.faas.openwhisk.scheduler.data.source.IObserver;
import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public abstract class Scheduler implements IObserver {

    @Override
    public abstract void register(@Nonnull List<ISubject> subjects);

    @Override
    public abstract void unregister(@Nonnull List<ISubject> subjects);

    @Override
    public abstract <T> void newEvent(@Nonnull UUID stream, @Nonnull final Collection<T> data);

}