# Tracing across the outbox and Kafka

## Problem
A trace starts in sim-service (for example at the REST activation) and reaches billing as a `traceparent` header on `sim.activated`. billing's own events leave through its outbox, sent later by a scheduled relay, outside that trace. Without help, `billing.charge.requested` would start a new trace.

## Decision
1. Micrometer Tracing with the OpenTelemetry bridge, exported over OTLP to Jaeger (`management.otlp.tracing.endpoint`; Jaeger runs from sim-service's `infra/docker-compose.yml`). Every request is sampled in this lab.
2. `spring.kafka.template.observation-enabled` and `spring.kafka.listener.observation-enabled` are on, so the listener continues the trace from the incoming `traceparent` and the template writes one on outgoing messages.
3. When an outbox row is written, `PostgresOutboxRepository` asks Micrometer's `Propagator` to inject the current span into a map (W3C `traceparent`, plus `tracestate` and baggage when present) and stores it as JSON in `outbox.trace_context` (migration V2).
4. `KafkaChargeEventPublisher` reads those headers back with `Propagator.extract`, opens a span `outbox relay <topic>` with that parent, and sends inside it.
5. Observations of `@Scheduled` runs are switched off (`ObservationConfiguration`); the relay runs every second.

Proven by `TracingKafkaTest`: a `sim.activated` with a given trace id produces a `billing.charge.requested` with the same trace id.

## Sources
- Debezium outbox event router: a `tracingspancontext` column carries the span context from the service that writes the outbox row to the component that publishes it.
- Spring for Apache Kafka reference, "Monitoring": observation on `KafkaTemplate` and listener containers.
- Spring Boot reference, "Tracing": Micrometer Tracing with the OpenTelemetry bridge and the OTLP exporter; `@AutoConfigureObservability` turns tracing on in tests.
