package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.EventOutcome;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresProcessedEventRepository implements ProcessedEventRepository {

    private final JpaProcessedEventEntityRepository jpa;

    public PostgresProcessedEventRepository(JpaProcessedEventEntityRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public boolean contains(UUID eventId) {
        return jpa.existsById(eventId);
    }

    @Override
    public void save(UUID eventId, EventOutcome outcome, Instant processedAt) {
        jpa.save(new ProcessedEventEntity(eventId, outcome, processedAt));
    }
}
