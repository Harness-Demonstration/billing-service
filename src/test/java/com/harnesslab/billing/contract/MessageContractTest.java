package com.harnesslab.billing.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.harnesslab.billing.repository.OutboxRepository;
import com.harnesslab.billing.repository.ProcessedEventRepository;
import com.harnesslab.billing.repository.SubscriptionRepository;
import com.harnesslab.billing.service.BillingService;
import com.harnesslab.billing.stream.SimEventReader;
import com.harnesslab.billing.types.ActivationFee;
import com.harnesslab.billing.types.EventOutcome;
import com.harnesslab.billing.types.OutboxEvent;
import com.harnesslab.billing.types.SimActivated;
import com.harnesslab.billing.types.SimDeactivated;
import com.harnesslab.billing.types.Subscription;
import com.networknt.schema.InputFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Consumer side: billing reads sim-service messages exactly as sim-service's contract defines them.
 * Producer side: billing.charge.requested as written to the outbox matches billing's own contract.
 * The sim-service contract comes from SIM_EVENTS_CONTRACT (CI checks out sim-service and sets it);
 * locally it defaults to the sibling folder ../sim-service.
 */
class MessageContractTest {

    private static final Path SIM_CONTRACT = Path.of(Optional.ofNullable(System.getenv("SIM_EVENTS_CONTRACT"))
            .orElse("../sim-service/docs/contracts/sim-events.asyncapi.yaml"));
    private static final Path BILLING_CONTRACT = Path.of("docs/contracts/billing-events.asyncapi.yaml");
    private static final String ICCID = "8949000000000000001";
    private static final String MSISDN = "491700000001";
    private static final UUID EVENT_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    private static final String FIX = " FIX: sim-service changed sim-events.asyncapi.yaml. Update the billing record in types, "
            + "SimEventReader and this fixture to the new shape, or ask sim-service to keep the old one.";

    private final ObjectMapper objectMapper = springObjectMapper();

    @Test
    void sim_activated_as_sim_service_defines_it_can_be_read() throws Exception {
        String json = "{\"eventId\":\"" + EVENT_ID + "\",\"iccid\":\"" + ICCID + "\",\"msisdn\":\"" + MSISDN
                + "\",\"activatedAt\":\"2026-01-02T03:04:05Z\"}";
        Set<ValidationMessage> errors = schema(SIM_CONTRACT, "SimActivated").validate(json, InputFormat.JSON);
        assertTrue(errors.isEmpty(), () -> "billing's sim.activated fixture no longer matches sim-service: " + errors + FIX);

        assertEquals(new SimActivated(EVENT_ID, ICCID, MSISDN, Instant.parse("2026-01-02T03:04:05Z")),
                new SimEventReader(objectMapper).activated(json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void sim_deactivated_as_sim_service_defines_it_can_be_read() throws Exception {
        String json = "{\"eventId\":\"" + EVENT_ID + "\",\"iccid\":\"" + ICCID + "\",\"msisdn\":\"" + MSISDN
                + "\",\"deactivatedAt\":\"2026-01-02T03:04:05Z\"}";
        Set<ValidationMessage> errors = schema(SIM_CONTRACT, "SimDeactivated").validate(json, InputFormat.JSON);
        assertTrue(errors.isEmpty(), () -> "billing's sim.deactivated fixture no longer matches sim-service: " + errors + FIX);

        assertEquals(new SimDeactivated(EVENT_ID, ICCID, MSISDN, Instant.parse("2026-01-02T03:04:05Z")),
                new SimEventReader(objectMapper).deactivated(json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void sim_service_still_publishes_the_topics_billing_listens_to() throws Exception {
        assertPublished(SimActivated.TOPIC, "SimActivated");
        assertPublished(SimDeactivated.TOPIC, "SimDeactivated");
    }

    @Test
    void billing_relies_only_on_fields_sim_service_guarantees() throws Exception {
        assertRequired("SimActivated", SimActivated.class);
        assertRequired("SimDeactivated", SimDeactivated.class);
    }

    @Test
    void charge_requested_as_sent_matches_the_billing_contract() throws Exception {
        List<OutboxEvent> saved = new ArrayList<>();
        BillingService service = new BillingService(
                new SubscriptionRepository() {
                    @Override
                    public Optional<Subscription> findByIccidForUpdate(String iccid) {
                        return Optional.empty();
                    }

                    @Override
                    public void save(Subscription subscription) {
                    }
                },
                new ProcessedEventRepository() {
                    @Override
                    public boolean contains(UUID eventId) {
                        return false;
                    }

                    @Override
                    public void save(UUID eventId, EventOutcome outcome, Instant processedAt) {
                    }
                },
                charge -> { },
                new OutboxRepository() {
                    @Override
                    public void save(OutboxEvent event) {
                        saved.add(event);
                    }

                    @Override
                    public List<OutboxEvent> lockPending(int limit) {
                        return List.of();
                    }
                },
                new ActivationFee(1000, "EUR"),
                objectMapper,
                Clock.fixed(Instant.parse("2026-01-02T03:04:05Z"), ZoneOffset.UTC));
        service.onActivated(new SimActivated(EVENT_ID, ICCID, MSISDN, Instant.parse("2026-01-01T00:00:00Z")));
        String json = saved.get(0).payload();

        Set<ValidationMessage> errors = schema(BILLING_CONTRACT, "ChargeRequested").validate(json, InputFormat.JSON);

        assertTrue(errors.isEmpty(), () -> "billing.charge.requested does not match the contract: " + errors + " in " + json);
    }

    private static void assertRequired(String message, Class<? extends Record> type) throws Exception {
        Set<String> required = new HashSet<>();
        payload(SIM_CONTRACT, message).path("required").forEach(field -> required.add(field.asText()));
        for (String field : Arrays.stream(type.getRecordComponents()).map(component -> component.getName()).toList()) {
            assertTrue(required.contains(field), () -> "billing reads " + message + "." + field
                    + " but sim-service no longer guarantees it (required: " + required + ")." + FIX);
        }
    }

    private static void assertPublished(String topic, String message) throws Exception {
        JsonNode contract = contract(SIM_CONTRACT);
        String channel = null;
        for (Map.Entry<String, JsonNode> entry : contract.path("channels").properties()) {
            if (topic.equals(entry.getValue().path("address").asText())) {
                channel = entry.getKey();
            }
        }
        assertTrue(channel != null, () -> "billing listens to " + topic + " but sim-service has no channel with that address."
                + FIX);
        String messageRef = "#/components/messages/" + message;
        boolean carriesMessage = false;
        for (JsonNode ref : contract.path("channels").path(channel).path("messages")) {
            carriesMessage |= messageRef.equals(ref.path("$ref").asText());
        }
        assertTrue(carriesMessage, () -> topic + " no longer carries " + message + " in sim-service's contract." + FIX);
        String channelRef = "#/channels/" + channel;
        boolean sent = false;
        for (JsonNode operation : contract.path("operations")) {
            sent |= "send".equals(operation.path("action").asText())
                    && channelRef.equals(operation.path("channel").path("$ref").asText());
        }
        assertTrue(sent, () -> "sim-service no longer has a send operation for " + topic + "." + FIX);
    }

    private static JsonNode contract(Path contract) throws Exception {
        assertTrue(Files.exists(contract), () -> contract.toAbsolutePath() + " not found. FIX: clone sim-service next to "
                + "billing-service or set SIM_EVENTS_CONTRACT to its docs/contracts/sim-events.asyncapi.yaml.");
        return new ObjectMapper(new YAMLFactory()).readTree(Files.readString(contract));
    }

    private static JsonNode payload(Path contract, String message) throws Exception {
        return contract(contract).at("/components/messages/" + message + "/payload");
    }

    private static JsonSchema schema(Path contract, String message) throws Exception {
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build();
        return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(payload(contract, message), config);
    }

    private static ObjectMapper springObjectMapper() {
        ObjectMapper[] holder = new ObjectMapper[1];
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
                .run(context -> holder[0] = context.getBean(ObjectMapper.class));
        return holder[0];
    }
}
