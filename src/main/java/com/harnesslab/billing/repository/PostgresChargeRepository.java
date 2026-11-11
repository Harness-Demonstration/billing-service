package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.Charge;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresChargeRepository implements ChargeRepository {

    private final JpaChargeEntityRepository jpa;

    public PostgresChargeRepository(JpaChargeEntityRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public void save(Charge charge) {
        jpa.save(new ChargeEntity(charge));
    }
}
