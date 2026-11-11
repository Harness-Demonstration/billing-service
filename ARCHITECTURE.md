# Architecture

This document describes the high-level architecture of billing-service. If you want to know where something is or what must stay true, start here. Names below are for symbol search, not links.

## Bird's Eye View

billing-service turns SIM lifecycle events into charges. sim-service announces activations on `sim.activated` and deactivations on `sim.deactivated`. billing keeps one subscription per SIM and, for every activation, requests the activation fee on `billing.charge.requested`.

The hard problems are delivery and order. Kafka delivers at least once, so the same event can arrive twice; billing must not charge twice. The two topics are separate, so a deactivation can arrive before the activation it follows; billing must not let an old event overwrite a newer state. Both rules are described in `docs/design-docs/idempotent-consumer.md`.

## Entry Points

- `SimEventsConsumer`: Kafka listeners for `sim.activated` and `sim.deactivated`.
- `OutboxRelay`: scheduled job that publishes pending outbox rows to `billing.charge.requested`.

## Code Map

All code lives under `com.harnesslab.billing`. Dependencies point down this list: `stream` uses `service`, `service` uses `repository`, everything may use `types`. Enforced by `LayerRulesTest.layers_depend_in_one_direction`.

### `types`

Plain records: the incoming `SimActivated` and `SimDeactivated`, the outgoing `ChargeRequested`, `Subscription`, `Charge`, `ActivationFee`, `OutboxEvent`, the `EventOutcome` stored per event, and `MalformedEventException`. No logic, no Spring, no JPA.

### `config`

Spring beans: the `Clock`, scheduling for the relay, the `ActivationFee` from `billing.activation-fee.*`, and the Kafka error handler that sends malformed messages to the dead-letter topic.

**Architecture Invariant:** no code calls `Instant.now()`; time comes from the injected `Clock`. Enforced by `LayerRulesTest.no_wall_clock`.

### `repository`

Persistence behind interfaces that `service` uses: `SubscriptionRepository`, `ProcessedEventRepository`, `ChargeRepository`, `OutboxRepository`. Implemented with Spring Data JPA on tables from `src/main/resources/db/migration`.

**Architecture Invariant:** JPA types (`jakarta.persistence`) never leave this package. Enforced by `LayerRulesTest`.

**Architecture Invariant:** a charge has a unique `source_event_id` and a processed event has a unique `event_id`, so the database refuses a second charge for the same event even if two consumers race.

### `service`

Every business decision. `BillingService` checks whether the eventId was processed, locks the subscription row, compares the event time with the newest change already applied, writes the subscription, the charge, the outbox row and the processed eventId in one transaction. `OutboxRelayService` publishes pending outbox rows and marks the acknowledged ones as sent.

**Architecture Invariant:** `service` never talks to Kafka. Enforced by `LayerRulesTest.service_does_not_use_kafka`.

### `stream`

Kafka in both directions. `SimEventReader` parses message bytes into records and rejects a message that misses a field billing needs. `SimEventsConsumer` calls one service method per message. `KafkaChargeEventPublisher` sends outbox payloads keyed by ICCID.

**Architecture Invariant:** no business logic here, and no access to `repository`. Enforced by `LayerRulesTest`.

## Cross-Cutting Concerns

### Contracts between repositories

billing reads messages it does not own. `MessageContractTest` validates billing's fixtures against sim-service's `sim-events.asyncapi.yaml` and checks that every field billing reads is required there. CI clones sim-service's main branch for this, so a contract change in sim-service turns billing's build red.

### Errors

A malformed message goes unchanged to `<topic>.billing-dlt` once and is never retried. Any other failure (database or Kafka unavailable) is retried every second, so no event is skipped.

### Observability

Micrometer Tracing with OpenTelemetry, exported to Jaeger. The listener continues the trace from `sim.activated`; the outbox row stores it and the relay continues it on `billing.charge.requested` (see `docs/design-docs/tracing.md`).

### Configuration

Database and Kafka addresses come from environment variables with local defaults (`application.yml`). The relay can be switched off with `billing.outbox.relay.enabled=false`.
