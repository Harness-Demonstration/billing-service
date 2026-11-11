package com.harnesslab.billing.types;

import java.time.Instant;
import java.util.UUID;

/**
 * An event waiting in, or already sent from, the outbox. sentAt is null until Kafka acknowledged it.
 * traceParent is the W3C traceparent of the span that wrote the row, or null; the relay continues that trace.
 */
public record OutboxEvent(
        UUID id, String aggregateId, String topic, String payload, Instant createdAt, Instant sentAt, String traceParent) {

    public OutboxEvent(UUID id, String aggregateId, String topic, String payload, Instant createdAt, Instant sentAt) {
        this(id, aggregateId, topic, payload, createdAt, sentAt, null);
    }
}
