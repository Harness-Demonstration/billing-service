# Tech debt tracker

Known gaps we chose not to fix yet. Each line says why it matters and what would fix it. Remove a line in the pull request that fixes it.

| Gap | Why it matters | Likely fix |
|---|---|---|
| Outbox rows that fail forever are retried every second with no limit | A permanently failing row is retried forever | Attempt counter, growing delay, a failed state after N attempts |
| The relay keeps its transaction open while waiting for Kafka acknowledgements (up to 10 seconds) | A slow Kafka holds a pooled connection and row locks | Claim rows with a lease, publish outside a transaction |
