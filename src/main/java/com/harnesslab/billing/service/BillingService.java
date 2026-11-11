package com.harnesslab.billing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies sim-service events to subscriptions. Every activation is charged exactly once, keyed by its eventId.
 * The subscription state only moves forward in time: an event older than the newest change already applied
 * to that SIM does not change it (see docs/product-specs/billing-charges.md).
 */
@Service
public class BillingService {

    private static final Logger log = LoggerFactory.getLogger(BillingService.class);

    private final SubscriptionRepository subscriptions;
    private final ProcessedEventRepository processedEvents;
    private final ChargeRepository charges;
    private final OutboxRepository outbox;
    private final ActivationFee fee;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public BillingService(
            SubscriptionRepository subscriptions,
            ProcessedEventRepository processedEvents,
            ChargeRepository charges,
            OutboxRepository outbox,
            ActivationFee fee,
            ObjectMapper objectMapper,
            Clock clock) {
        this.subscriptions = subscriptions;
        this.processedEvents = processedEvents;
        this.charges = charges;
        this.outbox = outbox;
        this.fee = fee;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public EventOutcome onActivated(SimActivated event) {
        if (processedEvents.contains(event.eventId())) {
            log.info("sim.activated {} for {} already processed, skipped", event.eventId(), event.iccid());
            return EventOutcome.DUPLICATE;
        }
        Optional<Subscription> current = subscriptions.findByIccidForUpdate(event.iccid());
        boolean newer = isNewer(event.activatedAt(), current);
        if (newer) {
            subscriptions.save(new Subscription(
                    event.iccid(), event.msisdn(), SubscriptionStatus.ACTIVE, event.activatedAt()));
        } else {
            log.info("sim.activated {} for {} is older than the last applied change, state kept",
                    event.eventId(), event.iccid());
        }
        requestActivationCharge(event);
        EventOutcome outcome = newer ? EventOutcome.ACTIVATED : EventOutcome.ACTIVATED_LATE;
        processedEvents.save(event.eventId(), outcome, clock.instant());
        return outcome;
    }

    @Transactional
    public EventOutcome onDeactivated(SimDeactivated event) {
        if (processedEvents.contains(event.eventId())) {
            log.info("sim.deactivated {} for {} already processed, skipped", event.eventId(), event.iccid());
            return EventOutcome.DUPLICATE;
        }
        Optional<Subscription> current = subscriptions.findByIccidForUpdate(event.iccid());
        EventOutcome outcome;
        if (isNewer(event.deactivatedAt(), current)) {
            subscriptions.save(new Subscription(
                    event.iccid(), event.msisdn(), SubscriptionStatus.ENDED, event.deactivatedAt()));
            outcome = EventOutcome.DEACTIVATED;
        } else {
            log.info("sim.deactivated {} for {} is older than the last applied change, ignored",
                    event.eventId(), event.iccid());
            outcome = EventOutcome.STALE;
        }
        processedEvents.save(event.eventId(), outcome, clock.instant());
        return outcome;
    }

    private static boolean isNewer(Instant happenedAt, Optional<Subscription> current) {
        return current.map(subscription -> happenedAt.isAfter(subscription.lastEventAt())).orElse(true);
    }

    private void requestActivationCharge(SimActivated event) {
        Instant now = clock.instant();
        Charge charge = new Charge(
                UUID.randomUUID(), event.eventId(), event.iccid(), event.msisdn(), fee.amountMinor(), fee.currency(), now);
        charges.save(charge);
        UUID messageId = UUID.randomUUID();
        ChargeRequested message = new ChargeRequested(
                messageId,
                charge.id(),
                charge.iccid(),
                charge.msisdn(),
                charge.amountMinor(),
                charge.currency(),
                ChargeRequested.ACTIVATION_FEE,
                now);
        outbox.save(new OutboxEvent(messageId, charge.iccid(), ChargeRequested.TOPIC, json(message), now, null));
    }

    private String json(ChargeRequested message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Cannot serialize " + ChargeRequested.TOPIC, exception);
        }
    }
}
