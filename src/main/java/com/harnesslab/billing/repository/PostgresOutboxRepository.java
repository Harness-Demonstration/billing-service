package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.OutboxEvent;
import java.util.List;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresOutboxRepository implements OutboxRepository {

    private final JpaOutboxEntityRepository jpa;

    public PostgresOutboxRepository(JpaOutboxEntityRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public void save(OutboxEvent event) {
        jpa.save(new OutboxEntity(event));
    }

    @Override
    public List<OutboxEvent> lockPending(int limit) {
        return jpa.findPending(PageRequest.of(0, limit)).stream().map(OutboxEntity::toDomain).toList();
    }
}
