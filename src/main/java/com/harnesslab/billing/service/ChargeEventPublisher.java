package com.harnesslab.billing.service;

import com.harnesslab.billing.types.OutboxEvent;
import java.util.concurrent.CompletableFuture;

public interface ChargeEventPublisher {
    /** Starts sending without waiting; the future completes when Kafka acknowledged the event, or fails. */
    CompletableFuture<Void> publish(OutboxEvent event);
}
