package com.harnesslab.billing.stream;

import com.harnesslab.billing.service.BillingService;
import com.harnesslab.billing.types.SimActivated;
import com.harnesslab.billing.types.SimDeactivated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SimEventsConsumer {

    private final SimEventReader reader;
    private final BillingService service;

    public SimEventsConsumer(SimEventReader reader, BillingService service) {
        this.reader = reader;
        this.service = service;
    }

    @KafkaListener(topics = SimActivated.TOPIC)
    public void onSimActivated(ConsumerRecord<byte[], byte[]> message) {
        service.onActivated(reader.activated(message.value()));
    }

    @KafkaListener(topics = SimDeactivated.TOPIC)
    public void onSimDeactivated(ConsumerRecord<byte[], byte[]> message) {
        service.onDeactivated(reader.deactivated(message.value()));
    }
}
