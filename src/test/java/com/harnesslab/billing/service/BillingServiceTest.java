package com.harnesslab.billing.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.harnesslab.billing.repository.ChargeRepository;
import com.harnesslab.billing.repository.OutboxRepository;
import com.harnesslab.billing.repository.ProcessedEventRepository;
import com.harnesslab.billing.repository.SubscriptionRepository;
import com.harnesslab.billing.types.ActivationFee;
import com.harnesslab.billing.types.Charge;
import com.harnesslab.billing.types.ChargeRequested;
import com.harnesslab.billing.types.EventOutcome;
import com.harnesslab.billing.types.OutboxEvent;
import com.harnesslab.billing.types.SimActivated;
import com.harnesslab.billing.types.SimDeactivated;
import com.harnesslab.billing.types.Subscription;
import com.harnesslab.billing.types.SubscriptionStatus;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BillingServiceTest {

    private static final String ICCID = "8949000000000000001";
    private static final String MSISDN = "491700000001";
    private static final Instant T1 = Instant.parse("2026-01-01T10:00:00Z");
    private static final Instant T2 = Instant.parse("2026-01-01T11:00:00Z");
    private static final Instant T3 = Instant.parse("2026-01-01T12:00:00Z");

    private final Map<String, Subscription> subscriptions = new HashMap<>();
    private final Map<UUID, EventOutcome> processed = new HashMap<>();
    private final List<Charge> charges = new ArrayList<>();
    private final List<OutboxEvent> outbox = new ArrayList<>();
    private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
    private final BillingService service = new BillingService(
            new SubscriptionRepository() {
                @Override
                public Optional<Subscription> findByIccidForUpdate(String iccid) {
                    return Optional.ofNullable(subscriptions.get(iccid));
                }

                @Override
                public void save(Subscription subscription) {
                    subscriptions.put(subscription.iccid(), subscription);
                }
            },
            new ProcessedEventRepository() {
                @Override
                public boolean contains(UUID eventId) {
                    return processed.containsKey(eventId);
                }

                @Override
                public void save(UUID eventId, EventOutcome outcome, Instant processedAt) {
                    processed.put(eventId, outcome);
                }
            },
            (ChargeRepository) charges::add,
            new OutboxRepository() {
                @Override
                public void save(OutboxEvent event) {
                    outbox.add(event);
                }

                @Override
                public List<OutboxEvent> lockPending(int limit) {
                    return List.of();
                }
            },
            new ActivationFee(1000, "EUR"),
            objectMapper,
            Clock.fixed(Instant.parse("2026-01-02T00:00:00Z"), ZoneOffset.UTC));

    @Test
    void b1_an_activation_requests_one_charge_of_the_activation_fee() throws Exception {
        UUID eventId = UUID.randomUUID();

        assertEquals(EventOutcome.ACTIVATED, service.onActivated(new SimActivated(eventId, ICCID, MSISDN, T1)));

        assertEquals(1, charges.size());
        assertEquals(eventId, charges.get(0).sourceEventId());
        assertEquals(1, outbox.size());
        ChargeRequested message = objectMapper.readValue(outbox.get(0).payload(), ChargeRequested.class);
        assertEquals(new ChargeRequested(outbox.get(0).id(), charges.get(0).id(), ICCID, MSISDN, 1000, "EUR",
                ChargeRequested.ACTIVATION_FEE, Instant.parse("2026-01-02T00:00:00Z")), message);
        assertEquals(ICCID, outbox.get(0).aggregateId());
        assertEquals(SubscriptionStatus.ACTIVE, subscriptions.get(ICCID).status());
    }

    @Test
    void b2_a_redelivered_activation_is_not_charged_again() {
        SimActivated event = new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T1);
        service.onActivated(event);

        assertEquals(EventOutcome.DUPLICATE, service.onActivated(event));

        assertEquals(1, charges.size());
        assertEquals(1, outbox.size());
    }

    @Test
    void b2_a_redelivered_deactivation_changes_nothing() {
        service.onActivated(new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T1));
        SimDeactivated deactivation = new SimDeactivated(UUID.randomUUID(), ICCID, MSISDN, T2);
        service.onDeactivated(deactivation);
        service.onActivated(new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T3));

        assertEquals(EventOutcome.DUPLICATE, service.onDeactivated(deactivation));

        assertEquals(SubscriptionStatus.ACTIVE, subscriptions.get(ICCID).status());
    }

    @Test
    void b3_a_deactivation_older_than_the_last_applied_change_is_ignored() {
        service.onActivated(new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T3));

        assertEquals(EventOutcome.STALE, service.onDeactivated(new SimDeactivated(UUID.randomUUID(), ICCID, MSISDN, T2)));

        assertEquals(new Subscription(ICCID, MSISDN, SubscriptionStatus.ACTIVE, T3), subscriptions.get(ICCID));
    }

    @Test
    void b3_a_late_activation_is_charged_but_does_not_reopen_an_ended_subscription() {
        service.onDeactivated(new SimDeactivated(UUID.randomUUID(), ICCID, MSISDN, T2));

        assertEquals(EventOutcome.ACTIVATED_LATE, service.onActivated(new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T1)));

        assertEquals(new Subscription(ICCID, MSISDN, SubscriptionStatus.ENDED, T2), subscriptions.get(ICCID));
        assertEquals(1, charges.size());
    }

    @Test
    void b3_an_event_at_the_same_time_as_the_last_applied_change_is_stale() {
        service.onActivated(new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T2));

        assertEquals(EventOutcome.STALE, service.onDeactivated(new SimDeactivated(UUID.randomUUID(), ICCID, MSISDN, T2)));
    }

    @Test
    void b4_a_deactivation_ends_the_subscription_and_a_later_activation_charges_again() {
        service.onActivated(new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T1));

        assertEquals(EventOutcome.DEACTIVATED, service.onDeactivated(new SimDeactivated(UUID.randomUUID(), ICCID, MSISDN, T2)));
        assertEquals(SubscriptionStatus.ENDED, subscriptions.get(ICCID).status());

        assertEquals(EventOutcome.ACTIVATED, service.onActivated(new SimActivated(UUID.randomUUID(), ICCID, MSISDN, T3)));
        assertEquals(SubscriptionStatus.ACTIVE, subscriptions.get(ICCID).status());
        assertEquals(2, charges.size());
    }
}
