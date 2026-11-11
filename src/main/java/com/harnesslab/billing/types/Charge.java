package com.harnesslab.billing.types;

import java.time.Instant;
import java.util.UUID;

/** One requested charge. sourceEventId is the sim.activated eventId that caused it; at most one charge per event. */
public record Charge(
        UUID id, UUID sourceEventId, String iccid, String msisdn, long amountMinor, String currency, Instant requestedAt) {
}
