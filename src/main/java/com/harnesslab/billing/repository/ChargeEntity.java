package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.Charge;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

@Entity
@Table(name = "charge")
public class ChargeEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    @Column(name = "source_event_id", nullable = false)
    private UUID sourceEventId;

    @Column(nullable = false)
    private String iccid;

    @Column(nullable = false)
    private String msisdn;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "requested_at", nullable = false)
    private Instant requestedAt;

    protected ChargeEntity() {}

    ChargeEntity(Charge charge) {
        this.id = charge.id();
        this.sourceEventId = charge.sourceEventId();
        this.iccid = charge.iccid();
        this.msisdn = charge.msisdn();
        this.amountMinor = charge.amountMinor();
        this.currency = charge.currency();
        this.requestedAt = charge.requestedAt();
    }

    @Override
    public UUID getId() {
        return id;
    }

    /** Charges are written once and never updated. */
    @Override
    public boolean isNew() {
        return true;
    }
}
