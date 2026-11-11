package com.harnesslab.billing.repository;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

public interface JpaSubscriptionEntityRepository extends JpaRepository<SubscriptionEntity, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from SubscriptionEntity s where s.iccid = :iccid")
    Optional<SubscriptionEntity> findForUpdate(String iccid);
}
