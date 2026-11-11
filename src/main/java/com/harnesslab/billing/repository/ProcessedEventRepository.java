package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.EventOutcome;
import java.time.Instant;
import java.util.UUID;

public interface ProcessedEventRepository {
    boolean contains(UUID eventId);

    /** Fails on commit if the eventId is already stored, so a concurrent duplicate rolls back and is retried. */
    void save(UUID eventId, EventOutcome outcome, Instant processedAt);
}
