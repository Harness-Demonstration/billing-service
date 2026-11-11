package com.harnesslab.billing.stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
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
class BillingKafkaTest {
    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16.4")
            .withDatabaseName("billing").withUsername("billing").withPassword("billing");
    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private static final String ICCID = "8949000000000000001";
    private static final String OTHER_ICCID = "8949000000000000002";
    private static final String MSISDN = "491700000001";

    @Autowired
    private JdbcTemplate jdbc;

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    @Test
    void b5_b6_kafka_events_are_charged_once_and_malformed_ones_reach_the_dead_letter_topic_unchanged() throws Exception {
        UUID activation = UUID.randomUUID();
        UUID deactivation = UUID.randomUUID();
        UUID staleDeactivation = UUID.randomUUID();
        UUID otherActivation = UUID.randomUUID();
        byte[] malformed = "{\"iccid\":\"8949000000000000003\"}".getBytes(StandardCharsets.UTF_8);

        try (KafkaProducer<byte[], byte[]> producer = producer()) {
            send(producer, "sim.activated", ICCID, activated(activation, ICCID, "2026-01-01T10:00:00Z"));
            waitFor(() -> count("select count(*) from processed_event where event_id = '" + activation + "'") == 1);
            send(producer, "sim.activated", ICCID, activated(activation, ICCID, "2026-01-01T10:00:00Z"));
            send(producer, "sim.deactivated", ICCID, deactivated(deactivation, "2026-01-01T12:00:00Z"));
            waitFor(() -> count("select count(*) from processed_event where event_id = '" + deactivation + "'") == 1);
            send(producer, "sim.deactivated", ICCID, deactivated(staleDeactivation, "2026-01-01T11:00:00Z"));
            producer.send(new ProducerRecord<>("sim.activated", key(ICCID), malformed)).get();
            send(producer, "sim.activated", OTHER_ICCID, activated(otherActivation, OTHER_ICCID, "2026-01-01T10:00:00Z"));
        }
        waitFor(() -> count("select count(*) from processed_event") == 4);

        assertEquals(2, count("select count(*) from charge"));
        assertEquals(1, count("select count(*) from charge where source_event_id = '" + activation + "'"));
        assertEquals("STALE", jdbc.queryForObject(
                "select outcome from processed_event where event_id = '" + staleDeactivation + "'", String.class));
        assertEquals("ENDED", jdbc.queryForObject(
                "select status from subscription where iccid = '" + ICCID + "'", String.class));

        List<ConsumerRecord<byte[], byte[]>> charges = read("billing.charge.requested", 2);
        assertEquals(2, charges.size());
        assertEquals(Set.of(ICCID, OTHER_ICCID), charges.stream()
                .map(record -> new String(record.key(), StandardCharsets.UTF_8)).collect(Collectors.toSet()));
        List<ConsumerRecord<byte[], byte[]>> deadLetters = read("sim.activated.billing-dlt", 1);
        assertEquals(1, deadLetters.size());
        assertArrayEquals(malformed, deadLetters.get(0).value());
        waitFor(() -> count("select count(*) from outbox where sent_at is null") == 0);
    }

    private static byte[] activated(UUID eventId, String iccid, String at) {
        return ("{\"eventId\":\"" + eventId + "\",\"iccid\":\"" + iccid + "\",\"msisdn\":\"" + MSISDN
                + "\",\"activatedAt\":\"" + at + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] deactivated(UUID eventId, String at) {
        return ("{\"eventId\":\"" + eventId + "\",\"iccid\":\"" + ICCID + "\",\"msisdn\":\"" + MSISDN
                + "\",\"deactivatedAt\":\"" + at + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] key(String iccid) {
        return iccid.getBytes(StandardCharsets.UTF_8);
    }

    private static void send(KafkaProducer<byte[], byte[]> producer, String topic, String iccid, byte[] value) throws Exception {
        producer.send(new ProducerRecord<>(topic, key(iccid), value)).get();
    }

    private static KafkaProducer<byte[], byte[]> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    private static List<ConsumerRecord<byte[], byte[]>> read(String topic, int expected) {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 60_000;
            while (records.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
            consumer.poll(Duration.ofSeconds(2)).forEach(records::add);
        }
        return records;
    }

    private int count(String sql) {
        return jdbc.queryForObject(sql, Integer.class);
    }

    private static void waitFor(Supplier<Boolean> condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (!condition.get()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 60 seconds");
            }
            Thread.sleep(200);
        }
    }
}
