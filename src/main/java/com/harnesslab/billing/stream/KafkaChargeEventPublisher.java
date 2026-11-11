package com.harnesslab.billing.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.harnesslab.billing.service.ChargeEventPublisher;
import com.harnesslab.billing.types.OutboxEvent;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaChargeEventPublisher implements ChargeEventPublisher {

    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final Propagator propagator;

    /** Without tracing (for example in tests) there is no Propagator bean; Propagator.NOOP reads no headers. */
    public KafkaChargeEventPublisher(
            KafkaTemplate<byte[], byte[]> kafkaTemplate,
            ObjectMapper objectMapper,
            Tracer tracer,
            ObjectProvider<Propagator> propagator) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.propagator = propagator.getIfAvailable(() -> Propagator.NOOP);
    }

    /**
     * Sends inside a span whose parent is the span that wrote the outbox row (the sim.activated listener),
     * so billing.charge.requested continues the trace that started in sim-service.
     */
    @Override
    public CompletableFuture<Void> publish(OutboxEvent event) {
        Map<String, String> headers = traceHeaders(event);
        if (headers.isEmpty()) {
            return send(event);
        }
        Span span = propagator.extract(headers, Map::get).name("outbox relay " + event.topic()).start();
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            return send(event);
        } finally {
            span.end();
        }
    }

    private Map<String, String> traceHeaders(OutboxEvent event) {
        if (event.traceContext() == null) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(event.traceContext(), new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException exception) {
            return Map.of();
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
