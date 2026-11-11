# billing-service

Consumes `sim.activated` and `sim.deactivated`, charges every activation once, publishes `billing.charge.requested`. Part of the harness lab, next to [sim-service](https://github.com/Harness-Demonstration/sim-service).

Build: clone sim-service next to this folder (its message contract is checked in the tests), then `./mvnw -q verify`.

Local run: start Kafka with `docker compose -f ../sim-service/infra/docker-compose.yml up -d kafka`, PostgreSQL with `docker compose -f infra/docker-compose.yml up -d`, then `./mvnw spring-boot:run`. The service listens on port 8081 and connects to `jdbc:postgresql://localhost:5434/billing` (user and password `billing`); override with `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD` and `KAFKA_BOOTSTRAP`.
