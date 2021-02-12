package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.data.source.ISubject;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.policy.IPolicy;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AdvancedScheduler extends Scheduler {

    private final Scheduler scheduler;

    public AdvancedScheduler(@Nonnull Scheduler scheduler) {
        checkNotNull(scheduler, "Scheduler to decorate can not be null.");
        this.scheduler = scheduler;
    }

    @Override
    public void register(@Nonnull List<ISubject> subjects) {
        scheduler.register(subjects);
    }

    @Override
    public void unregister(@Nonnull List<ISubject> subjects) {
        scheduler.unregister(subjects);
    }

    @Override
    public void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data) {
        scheduler.newEvent(stream, data);
    }

    @Override
    public IPolicy getPolicy() {
        return scheduler.getPolicy();
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
    }

}