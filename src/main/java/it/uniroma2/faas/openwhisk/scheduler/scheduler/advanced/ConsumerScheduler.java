package it.uniroma2.faas.openwhisk.scheduler.scheduler.advanced;

import it.uniroma2.faas.openwhisk.scheduler.scheduler.Scheduler;

import javax.annotation.Nonnull;

/**
 * AdvancedScheduler which create all the needed event sources and forward events to super.Scheduler.
 */
public class ConsumerScheduler extends AdvancedScheduler {

    public ConsumerScheduler(@Nonnull Scheduler scheduler) {
        super(scheduler);
    }

}