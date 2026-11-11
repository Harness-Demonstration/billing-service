# Idempotent consumer with stale-event protection

## Problem
sim-service publishes through a transactional outbox, so delivery is at least once: the same `sim.activated` can arrive twice with the same `eventId`. `sim.activated` and `sim.deactivated` are separate topics, so their relative order is not guaranteed: a deactivation can arrive before the activation that came first.

## Decision
1. **Duplicates.** Every applied event's `eventId` is stored in `processed_event` in the same transaction as its effects. An event whose `eventId` is already stored changes nothing. A unique index on `charge.source_event_id` is a second guard: a racing duplicate fails on commit, rolls back and is retried, and then finds the stored `eventId`.
2. **Stale events.** The subscription stores `last_event_at`, the time of the newest change applied. An event that happened at or before that time does not change the subscription. The subscription row is locked (`select ... for update`) while this is decided.
3. **Late activations are still charged.** An activation that arrives after a newer deactivation did happen, so its fee is requested; only the subscription state is kept.
4. **Malformed messages** go unchanged to `<topic>.billing-dlt` and are never retried, so one bad message never blocks the partition.

## Sources
- microservices.io, "Idempotent Consumer" pattern: record processed message ids in the same transaction as the business change.
- microservices.io, "Transactional outbox": delivery is at least once, so consumers must be idempotent.
- Spring for Apache Kafka reference, `DefaultErrorHandler` and `DeadLetterPublishingRecoverer`, with not-retryable exceptions.

## Known limits
- Comparing event times assumes sim-service's clock is the only source of these times; it is.
- A deactivation for a SIM billing has never seen creates an ended subscription, so an older activation that arrives later is charged but does not reopen it.
