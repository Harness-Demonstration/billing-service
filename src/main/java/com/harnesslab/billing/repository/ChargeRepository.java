package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.Charge;

public interface ChargeRepository {
    void save(Charge charge);
}
