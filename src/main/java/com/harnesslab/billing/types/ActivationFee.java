package com.harnesslab.billing.types;

/** Price of one activation, in minor units (cents) of an ISO 4217 currency. */
public record ActivationFee(long amountMinor, String currency) {
}
