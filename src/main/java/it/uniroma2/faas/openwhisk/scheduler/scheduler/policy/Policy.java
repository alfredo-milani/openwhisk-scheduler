package it.uniroma2.faas.openwhisk.scheduler.scheduler.policy;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Note: the {@link IPolicy} object should be idempotent.
 */
public enum Policy {

    PASS_THROUGH,
    PRIORITY_QUEUE_FIFO,
    SHORTEST_JOB_FIRST;

    public static @Nonnull Policy from(@Nonnull String policy) {
        checkNotNull(policy, "Policy can not be null.");

        for (Policy p : Policy.values()) {
            if (policy.equalsIgnoreCase(p.name())) {
                return p;
            }
        }

        throw new TypeNotPresentException(policy, new Throwable("Selected policy not yet implemented."));
    }

}
