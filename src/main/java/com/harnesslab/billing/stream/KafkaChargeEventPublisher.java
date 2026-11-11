package com.harnesslab.billing.stream;

import com.harnesslab.billing.service.ChargeEventPublisher;
import com.harnesslab.billing.types.OutboxEvent;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaChargeEventPublisher implements ChargeEventPublisher {

    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;

    public KafkaChargeEventPublisher(KafkaTemplate<byte[], byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public CompletableFuture<Void> publish(OutboxEvent event) {
        try {
            return kafkaTemplate.send(event.topic(), event.aggregateId().getBytes(StandardCharsets.UTF_8),
                    event.payload().getBytes(StandardCharsets.UTF_8)).thenApply(result -> null);
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }
}
