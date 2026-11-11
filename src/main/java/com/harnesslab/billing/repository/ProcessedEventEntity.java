package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.EventOutcome;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

@Entity
@Table(name = "processed_event")
public class ProcessedEventEntity implements Persistable<UUID> {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventOutcome outcome;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEventEntity() {}

    ProcessedEventEntity(UUID eventId, EventOutcome outcome, Instant processedAt) {
        this.eventId = eventId;
        this.outcome = outcome;
        this.processedAt = processedAt;
    }

    @Override
    public UUID getId() {
        return eventId;
    }

    /** Always an insert: a second row with the same eventId must fail, never overwrite the first. */
    @Override
    public boolean isNew() {
        return true;
    }
}
