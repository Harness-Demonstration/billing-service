package com.harnesslab.billing.repository;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JpaProcessedEventEntityRepository extends JpaRepository<ProcessedEventEntity, UUID> {
}
