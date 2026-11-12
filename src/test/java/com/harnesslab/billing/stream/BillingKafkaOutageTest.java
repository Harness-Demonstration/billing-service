package com.harnesslab.billing.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.harnesslab.billing.service.BillingService;
import com.harnesslab.billing.types.SimActivated;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SpringBootTest
class BillingKafkaOutageTest {
    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16.4")
            .withDatabaseName("billing").withUsername("billing").withPassword("billing");
    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private static final String ICCID = "8949000000000000041";
    private static final String TOPIC = "billing.charge.requested";

    @Autowired
    private BillingService service;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    @Test
    @Timeout(value = 110, unit = TimeUnit.SECONDS)
    void b6_charge_request_reaches_kafka_after_an_outage() throws Exception {
        UUID eventId = UUID.randomUUID();
        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
        try {
            service.onActivated(new SimActivated(
                    eventId, ICCID, "491700000041", Instant.parse("2026-01-01T10:00:00Z")));
            Thread.sleep(2500);

            assertEquals(1, jdbc.queryForObject(
                    "select count(*) from charge where source_event_id = ?", Integer.class, eventId));
            assertEquals(1, countOutbox("sent_at is null"));
            assertEquals(1, countOutbox("true"));
        } finally {
            KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        while (countOutbox("sent_at is not null") != 1 && System.nanoTime() < deadline) {
            Thread.sleep(200);
        }
        assertEquals(1, countOutbox("sent_at is not null"));

        List<ConsumerRecord<byte[], byte[]>> records = readCharges();
        assertEquals(1, records.size());
        String payload = jdbc.queryForObject(
                "select payload::text from outbox where aggregate_id = ? and topic = ?", String.class, ICCID, TOPIC);
        assertEquals(objectMapper.readTree(payload), objectMapper.readTree(records.get(0).value()));
    }

    private int countOutbox(String condition) {
        return jdbc.queryForObject("select count(*) from outbox where aggregate_id = ? and topic = ? and "
                + condition, Integer.class, ICCID, TOPIC);
    }

    private static List<ConsumerRecord<byte[], byte[]>> readCharges() {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                    if (ICCID.equals(new String(record.key(), StandardCharsets.UTF_8))) {
                        records.add(record);
                        if (records.size() == 1) {
                            deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
                        }
                    }
                }
            }
        }
        return records;
    }
}
