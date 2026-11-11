# Billing charges

Owner: Billing team.

## Why
A customer pays the activation fee once for every activation of their SIM: never twice for the same activation, never missed, and a deactivated SIM must not look active because an old message arrived late.

## Criteria
IDs never change and are never reused. A new criterion takes the next free number. Each test is named after the ID it proves.

| ID | Criterion | Status |
|---|---|---|
| B1 | Every `sim.activated` event requests one charge of the configured activation fee on `billing.charge.requested`, keyed by ICCID, with reason `ACTIVATION_FEE`. | done |
| B2 | A redelivered `sim.activated` or `sim.deactivated` (same `eventId`) changes nothing and requests no charge. | done |
| B3 | An event that happened at or before the newest change already applied to that SIM does not change the subscription. A late activation is still charged once; a late deactivation is ignored. | done |
| B4 | A deactivation ends the subscription; a later activation makes it active again and is charged again. | done |
| B5 | A message that misses a field billing needs, or is not readable JSON, changes nothing and is delivered unchanged to `<topic>.billing-dlt`; later messages are processed. | done |
| B6 | A charge request is written to the outbox in the same transaction as the change that caused it and is published to Kafka at least once. | done |

## Retired criteria
| ID | Was | Replaced by |
|---|---|---|
| (none yet) | | |

## Decision log
- 2026-11-11: Late activations are charged but do not change the subscription state, see [idempotent consumer](../design-docs/idempotent-consumer.md).
