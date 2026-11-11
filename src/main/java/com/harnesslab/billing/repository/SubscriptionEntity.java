package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.Subscription;
import com.harnesslab.billing.types.SubscriptionStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "subscription")
public class SubscriptionEntity {

    @Id
    private String iccid;

    @Column(nullable = false)
    private String msisdn;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SubscriptionStatus status;

    @Column(name = "last_event_at", nullable = false)
    private Instant lastEventAt;

    protected SubscriptionEntity() {}

    SubscriptionEntity(Subscription subscription) {
        this.iccid = subscription.iccid();
        this.msisdn = subscription.msisdn();
        this.status = subscription.status();
        this.lastEventAt = subscription.lastEventAt();
    }

    Subscription toDomain() {
        return new Subscription(iccid, msisdn, status, lastEventAt);
    }
}
