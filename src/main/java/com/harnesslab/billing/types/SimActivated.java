package com.harnesslab.billing.types;

import java.time.Instant;
import java.util.UUID;

/** Message read from sim.activated, owned by sim-service. eventId is the same on a redelivery. */
public record SimActivated(UUID eventId, String iccid, String msisdn, Instant activatedAt) {
    public static final String TOPIC = "sim.activated";
}
