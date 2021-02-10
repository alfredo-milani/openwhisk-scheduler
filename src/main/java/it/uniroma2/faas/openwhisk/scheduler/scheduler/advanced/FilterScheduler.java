package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model.ISchedulable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilterScheduler extends AdvancedScheduler {

    private static final Logger LOG = LogManager.getLogger(FilterScheduler.class.getCanonicalName());

    public static final Class<? extends ISchedulable> DEFAULT_FILTER = ISchedulable.class;

    private Class<? extends ISchedulable> filter = DEFAULT_FILTER;

    public FilterScheduler(@Nonnull Scheduler scheduler) {
        super(scheduler);
    }

    @Override
    public void newEvent(@Nonnull final UUID stream, @Nonnull final Collection<?> data) {
        checkNotNull(stream, "Stream can not be null.");
        checkNotNull(data, "Data can not be null.");

        Collection<ISchedulable> schedulables = data.stream()
                .filter(filter::isInstance)
                .map(filter::cast)
                .collect(Collectors.toUnmodifiableList());
        if (schedulables.size() != data.size()) {
            LOG.trace("Filtered {} element.", data.size() - schedulables.size());
        }
        super.newEvent(stream, schedulables);
    }

    public void setFilter(@Nonnull Class<? extends ISchedulable> filter) {
        checkNotNull(filter, "Filter can not be null.");
        this.filter = filter;
    }

}