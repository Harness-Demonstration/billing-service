# Design docs

Every design document in this folder is listed here with its status. A document that is not listed here fails the docs check in CI.

| Document | What it decides | Status | Last verified against the code |
|---|---|---|---|
| [core-beliefs.md](core-beliefs.md) | How humans and agents work in this repository | accepted | 2026-11-11 |
| [tracing.md](tracing.md) | How the trace from sim-service continues through billing's outbox | accepted | 2026-11-11 |
| [idempotent-consumer.md](idempotent-consumer.md) | How duplicate, late and malformed events are handled | accepted | 2026-11-11 |

Statuses: `proposed` (under discussion), `accepted` (the code follows it), `superseded` (replaced; say by what).
