package com.harnesslab.billing.repository;

import com.harnesslab.billing.types.OutboxEvent;
import java.util.List;

public interface OutboxRepository {
    void save(OutboxEvent event);

    /** Oldest unsent events, locked with SKIP LOCKED so two relays never take the same row. */
    List<OutboxEvent> lockPending(int limit);
}
