package com.harnesslab.billing.types;

import java.time.Instant;
import java.util.UUID;

/** Message on billing.charge.requested. eventId is the same on a redelivery; chargeId identifies the charge. */
public record ChargeRequested(
        UUID eventId,
        UUID chargeId,
        String iccid,
        String msisdn,
        long amountMinor,
        String currency,
        String reason,
        Instant requestedAt) {
    public static final String TOPIC = "billing.charge.requested";
    public static final String ACTIVATION_FEE = "ACTIVATION_FEE";
}
