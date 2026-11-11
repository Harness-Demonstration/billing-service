package com.harnesslab.billing.stream;

import com.harnesslab.billing.service.ChargeEventPublisher;
import com.harnesslab.billing.types.OutboxEvent;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaChargeEventPublisher implements ChargeEventPublisher {

    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final Tracer tracer;

    public KafkaChargeEventPublisher(KafkaTemplate<byte[], byte[]> kafkaTemplate, ObjectProvider<Tracer> tracer) {
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer.getIfAvailable(() -> Tracer.NOOP);
    }

    /**
     * Sends inside a span whose parent is the span that wrote the outbox row (the sim.activated listener),
     * so billing.charge.requested continues the trace that started in sim-service.
     */
    @Override
    public CompletableFuture<Void> publish(OutboxEvent event) {
        String[] parts = event.traceParent() == null ? new String[0] : event.traceParent().split("-");
        if (parts.length != 4) {
            return send(event);
        }
        TraceContext parent = tracer.traceContextBuilder()
                .traceId(parts[1]).spanId(parts[2]).sampled("01".equals(parts[3])).build();
        Span span = tracer.spanBuilder().setParent(parent).name("outbox relay " + event.topic()).start();
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            return send(event);
        } finally {
            span.end();
        }
    }

    private CompletableFuture<Void> send(OutboxEvent event) {
        try {
            return kafkaTemplate.send(event.topic(), event.aggregateId().getBytes(StandardCharsets.UTF_8),
                    event.payload().getBytes(StandardCharsets.UTF_8)).thenApply(result -> null);
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }
}
