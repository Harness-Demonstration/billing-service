package com.harnesslab.billing.stream;

import com.harnesslab.billing.service.OutboxRelayService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "billing.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private final OutboxRelayService service;
    private final KafkaChargeEventPublisher publisher;

    public OutboxRelay(OutboxRelayService service, KafkaChargeEventPublisher publisher) {
        this.service = service;
        this.publisher = publisher;
    }

    @Scheduled(fixedDelay = 1000)
    public void relay() {
        service.relayPending(publisher);
    }
}
