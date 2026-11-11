# Testing in billing-service

## Naming
- A test that proves a spec criterion starts with the criterion's ID in lower case, followed by the behaviour: `b2_a_redelivered_activation_is_not_charged_again`. A test that proves several criteria lists them: `b5_b6_...`.
- Acceptance criteria of one-off tasks stay in the issue; their tests are named after the behaviour, without an ID.
- When a change makes an existing criterion wrong, retire it in the spec and delete or rewrite its test in the same pull request.

## Kinds of tests
| Kind | Uses | When |
|---|---|---|
| Unit | plain JUnit, fakes declared inside the test class, fixed `Clock` | business rules in `service` |
| Kafka integration | real PostgreSQL and Kafka from Testcontainers | consumers, the relay, the dead-letter topic |
| Contract | JSON Schema from both AsyncAPI files, `MessageContractTest` | every message billing reads or sends |
| Architecture | ArchUnit in `LayerRulesTest` | every build |

## The sim-service contract
`MessageContractTest` reads sim-service's `docs/contracts/sim-events.asyncapi.yaml` from `SIM_EVENTS_CONTRACT`, or from `../sim-service` when the variable is not set. CI and the Copilot setup steps clone sim-service there. If the test fails with "sim-service changed", update billing to the new shape; never edit sim-service's contract from here.

## Testcontainers
- The version comes from the `testcontainers.version` property in `pom.xml`. Never write a version on the individual dependencies.
- Images are pinned: `postgres:16.4`, `apache/kafka:3.8.0`.

## Test data
Synthetic values only: ICCIDs like `8949000000000000001`, MSISDNs like `491700000001`. Never real numbers.

## Deleting a test
Only when the behaviour it proves is intentionally changed. Say so in the pull request. Deleting a test to make the build green is never allowed.
