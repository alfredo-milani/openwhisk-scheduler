package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BufferedScheduler extends AdvancedScheduler {

    private final static Logger LOG = LogManager.getLogger(BufferedScheduler.class.getCanonicalName());

    private float threshold;

    public BufferedScheduler(Scheduler scheduler) {
        super(scheduler);
    }

    @Override
    public <T> void newEvent(@Nonnull UUID stream, @Nonnull final Collection<T> data) {
        checkNotNull(stream, "Stream can not be null.");
        checkNotNull(data, "Data can not be null.");

        super.newEvent(stream, data);
    }

    public void setThreshold(float threshold) {
        checkArgument(threshold >= 0 && threshold <= 100,
                "Threshold must be >= 0 and <= 100.");
        this.threshold = threshold;
    }

}