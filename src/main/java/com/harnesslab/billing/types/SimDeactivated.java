package com.harnesslab.billing.types;

import java.time.Instant;
import java.util.UUID;

/** Message read from sim.deactivated, owned by sim-service. eventId is the same on a redelivery. */
public record SimDeactivated(UUID eventId, String iccid, String msisdn, Instant deactivatedAt) {
    public static final String TOPIC = "sim.deactivated";
}
