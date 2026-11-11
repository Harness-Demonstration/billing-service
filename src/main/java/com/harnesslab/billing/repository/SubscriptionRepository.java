package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.Subscription;
import java.util.Optional;

public interface SubscriptionRepository {
    /** Reads the subscription and locks its row until the transaction ends. */
    Optional<Subscription> findByIccidForUpdate(String iccid);

    void save(Subscription subscription);
}
