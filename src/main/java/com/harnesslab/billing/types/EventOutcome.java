package com.harnesslab.billing.types;

/** What billing did with one incoming event. Stored per eventId, except DUPLICATE. */
public enum EventOutcome {
    /** Activation applied: the subscription is active and a charge was requested. */
    ACTIVATED,
    /** Activation older than the newest change already applied: charged, but the subscription state was kept. */
    ACTIVATED_LATE,
    /** Deactivation applied: the subscription ended. */
    DEACTIVATED,
    /** Deactivation older than the newest change already applied: ignored. */
    STALE,
    /** This eventId was processed before; nothing changed. */
    DUPLICATE
}
