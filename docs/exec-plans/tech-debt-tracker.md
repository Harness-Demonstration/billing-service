# Tech debt tracker

Known gaps we chose not to fix yet. Each line says why it matters and what would fix it. Remove a line in the pull request that fixes it.

| Gap | Why it matters | Likely fix |
|---|---|---|
| Outbox rows that fail forever are retried every second with no limit | A permanently failing row is retried forever | Attempt counter, growing delay, a failed state after N attempts |
| The relay keeps its transaction open while waiting for Kafka acknowledgements (up to 10 seconds) | A slow Kafka holds a pooled connection and row locks | Claim rows with a lease, publish outside a transaction |
| No test that a charge request survives Kafka being down | B6 is proven only with Kafka up | Stop the Kafka container in a test, activate, start it, assert the message arrives |
| billing's CI notices a sim-service contract change only when billing builds | A breaking change can merge in sim-service first | Run billing's `MessageContractTest` in sim-service's CI against the pull request's contract |
