package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.Subscription;
import java.util.Optional;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresSubscriptionRepository implements SubscriptionRepository {

    private final JpaSubscriptionEntityRepository jpa;

    public PostgresSubscriptionRepository(JpaSubscriptionEntityRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public Optional<Subscription> findByIccidForUpdate(String iccid) {
        return jpa.findForUpdate(iccid).map(SubscriptionEntity::toDomain);
    }

    @Override
    public void save(Subscription subscription) {
        jpa.save(new SubscriptionEntity(subscription));
    }
}
