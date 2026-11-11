package com.harnesslab.billing.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** A charge request continues the trace of the sim.activated message that caused it (docs/design-docs/tracing.md). */
@Testcontainers
@AutoConfigureObservability
@SpringBootTest
class TracingKafkaTest {
    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16.4")
            .withDatabaseName("billing").withUsername("billing").withPassword("billing");
    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";

    @TestConfiguration
    static class Exporter {
        @Bean
        InMemorySpanExporter inMemorySpanExporter() {
            return InMemorySpanExporter.create();
        }
    }

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
        registry.add("management.otlp.tracing.endpoint", () -> "http://localhost:1/v1/traces");
    }

    @Test
    void charge_requested_continues_the_trace_of_sim_activated() throws Exception {
        String iccid = "8949000000000000031";
        byte[] value = ("{\"eventId\":\"" + UUID.randomUUID() + "\",\"iccid\":\"" + iccid
                + "\",\"msisdn\":\"491700000031\",\"activatedAt\":\"2026-01-01T10:00:00Z\"}").getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> activated = new ProducerRecord<>("sim.activated", iccid.getBytes(StandardCharsets.UTF_8), value);
        activated.headers().add("traceparent", ("00-" + TRACE_ID + "-00f067aa0ba902b7-01").getBytes(StandardCharsets.UTF_8));
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(activated).get();
        }

        Header header = firstMessage("billing.charge.requested").headers().lastHeader("traceparent");

        assertNotNull(header, "billing.charge.requested has no traceparent header");
        assertEquals(TRACE_ID, new String(header.value(), StandardCharsets.UTF_8).split("-")[1]);
    }

    private static ConsumerRecord<byte[], byte[]> firstMessage(String topic) {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                    return record;
                }
            }
        }
        throw new AssertionError("no message on " + topic + " within 60 seconds");
    }
}
