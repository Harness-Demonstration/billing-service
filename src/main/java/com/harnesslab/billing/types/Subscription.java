package com.harnesslab.billing.types;

import java.time.Instant;

/**
 * Billing's view of one SIM. lastEventAt is the time of the newest state change applied so far; an event
 * that happened before it is stale and is ignored.
 */
public record Subscription(String iccid, String msisdn, SubscriptionStatus status, Instant lastEventAt) {
}
