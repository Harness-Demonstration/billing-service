# billing-service: the map

Read this first. It stays under 100 lines and tells you where to look next. Open only the documents your task needs.

## What this service does
Consumes `sim.activated` and `sim.deactivated` from sim-service, keeps one subscription per SIM in PostgreSQL, and requests the activation fee on `billing.charge.requested`.
Money depends on it: every activation is charged exactly once, a redelivered event is never charged again, and an old event never overwrites a newer state.

Stack: Java 17, Spring Boot 3.3, Spring Data JPA, Flyway, PostgreSQL 16, Spring Kafka with Kafka 3.8, Maven wrapper.

## Where to look
| You need | Read |
|---|---|
| Packages, what may depend on what, invariants | `ARCHITECTURE.md` |
| How we work with agents | `docs/design-docs/core-beliefs.md` |
| Lasting behaviour the service guarantees, with IDs (B1, B2 ...) | `docs/product-specs/index.md` |
| Design decisions, for example duplicate and stale events | `docs/design-docs/index.md` |
| How to name and write tests, Testcontainers versions | `docs/testing.md` |
| Planning a complex feature or a significant refactor | `docs/PLANS.md`, plans in `docs/exec-plans/` |
| Known gaps we chose not to fix yet | `docs/exec-plans/tech-debt-tracker.md` |
| Shape of `billing.charge.requested` | `docs/contracts/billing-events.asyncapi.yaml` |
| Shape of `sim.activated` and `sim.deactivated` | `../sim-service/docs/contracts/sim-events.asyncapi.yaml` (sim-service cloned next to this repository) |
| How a trace continues through the outbox and Kafka | `docs/design-docs/tracing.md` |
| How issues reach you and how reviews come back | https://github.com/Harness-Demonstration/harness/blob/main/docs/agent-loop.md |
| Local PostgreSQL for billing | `infra/docker-compose.yml` |

## How to work
1. If the issue adds or changes lasting behaviour, write it into `docs/product-specs/billing-charges.md` first, with the next free ID. Technical decisions go to `ARCHITECTURE.md` or `docs/design-docs/`.
2. Write the test first. A test that proves a spec criterion starts with its ID, for example `b2_a_redelivered_activation_is_not_charged_again`. Details in `docs/testing.md`.
3. Put every decision in `service`. Consumers parse the message and call one service method.
4. Publish events only through the outbox: the outbox row is written in the same transaction as the change that caused it.
5. Persistence uses Spring Data JPA; entities stay in `repository` and map to `types` records; hand-written SQL only in Flyway migrations.
6. Take the time from the injected `java.time.Clock`.
7. Before you open or update the pull request, clone sim-service next to this repository if it is missing (`git clone https://github.com/Harness-Demonstration/sim-service.git ../sim-service`), then run `./mvnw -q verify` (needs Docker) and `scripts/check-docs.sh`.
8. In the pull request description, list each acceptance criterion with the test that proves it.
9. If you add a document under `docs/`, link it from the matching `index.md` or from this map.
10. A message billing publishes changes in `docs/contracts/billing-events.asyncapi.yaml` first, then in the code. Messages billing reads belong to sim-service; billing never changes their contract.
11. When asked to address review comments, fix each one or reply to it with the reason the code stays as it is.

## Never
- Never delete or weaken a test to make the build green. A test is removed only when the behaviour it proves is intentionally changed, and the pull request says so.
- Never put real phone numbers or ICCIDs in tests.
- Never charge without recording the eventId that caused the charge in the same transaction.
