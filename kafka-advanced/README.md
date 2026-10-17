# kafka-advanced

Production-grade Apache Kafka microservices covering advanced concepts:
Idempotent Producer, Partitioning, Retry, Dead Letter Topic, Kafka Streams, Schema Registry, and Transactions.

---

## Services

| Service | Port | Concept | Status |
|---|---|---|---|
| [base-domain](./base-domain/) | — | Shared DTOs & Enums | ✅ Phase 1 |
| [order-service](./order-service/) | 8081 | Idempotent Producer, Partition by key | ✅ Phase 1 |
| [payment-service](./payment-service/) | 8082 | Retry 3x + Dead Letter Topic + PostgreSQL | ✅ Phase 1 |
| [shipping-service](./shipping-service/) | 8083 | Global Error Handler | ✅ Phase 1 |
| [dlt-handler-service](./dlt-handler-service/) | 8085 | DLT Consumer + Failure Alerting | ✅ Phase 1 |
| audit-service | 8084 | Kafka Streams — real-time aggregation | ⬜ Phase 2 |

---

## Architecture

```
[REST Client]
      │
      ▼
order-service (8081)
Idempotent Producer
Partition by customerId
      │
      ▼
[ Kafka: order-events — 3 partitions ]
      │
      ├──────────────────────────────┐──────────────────────────────┐
      ▼                              ▼                              ▼
payment-service (8082)    shipping-service (8083)         audit-service (8084)
Retry 3x + DLT            Global Error Handler            Kafka Streams
Save to PostgreSQL                                        [Phase 2]
      │
      │ on failure after 3 retries
      ▼
[ Kafka: order-events-dlt ]
      │
      ▼
dlt-handler-service (8085)
Log + Alert + Manual intervention
```

---

## Kafka Concepts Covered

| # | Concept | Service | Description |
|---|---|---|---|
| 1 | Idempotent Producer | order-service | No duplicate messages on retry |
| 2 | Partition by Key | order-service | Message order guaranteed per customer |
| 3 | Retry + Backoff | payment-service | 3 retries: 1s → 2s → 4s |
| 4 | Dead Letter Topic | payment-service | Failed messages go to DLT after retries |
| 5 | DLT Consumer | dlt-handler-service | Handles failed messages, alerts ops |
| 6 | Global Error Handler | shipping-service | Centralized error handling for all listeners |
| 7 | Kafka Streams | audit-service | Real-time aggregation (Phase 2) |
| 8 | Schema Registry + Avro | all | Replace JSON with Avro (Phase 2) |
| 9 | Kafka Transactions | order + payment | Exactly-once guarantee (Phase 3) |
| 10 | Kafka Connect | — | DB sink connector (Phase 3) |
| 11 | SSL/SASL Security | all | Broker auth (Phase 3) |

---

## Quick Start

### Step 1 — Start infrastructure

```bash
docker compose up -d
```

Starts: Kafka, Zookeeper, Kafka UI, PostgreSQL, Elasticsearch, Logstash, Kibana, Zipkin

### Step 2 — Build all services

```bash
cd base-domain        && mvn clean install -q && cd ..
cd order-service      && mvn clean install -q && cd ..
cd payment-service    && mvn clean install -q && cd ..
cd shipping-service   && mvn clean install -q && cd ..
cd dlt-handler-service && mvn clean install -q && cd ..
```

### Step 3 — Start all services (5 terminals)

```bash
java -jar order-service/target/order-service-1.0.0.jar
java -jar payment-service/target/payment-service-1.0.0.jar
java -jar shipping-service/target/shipping-service-1.0.0.jar
java -jar dlt-handler-service/target/dlt-handler-service-1.0.0.jar
```

---

## Test Scenarios

### Normal Order (success flow)

```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "productId": "PROD-101",
    "productName": "iPhone 15",
    "quantity": 2,
    "price": 999.99
  }'
```

Expected: Payment processed → saved to DB → shipment scheduled

---

### Retry + DLT Demo (price = 1 triggers failure)

```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-002",
    "productId": "PROD-102",
    "productName": "Fail Test",
    "quantity": 1,
    "price": 1
  }'
```

Expected: payment-service retries 3x → message goes to DLT → dlt-handler-service logs error

---

### Partition Test (same customer = same partition)

Send 3 orders with same `customerId` — all will go to the same Kafka partition.
Check Kafka UI → Topics → order-events → Messages to verify.

---

## Infrastructure URLs

| Tool | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Kibana | http://localhost:5601 |
| Zipkin | http://localhost:9411 |
| Elasticsearch | http://localhost:9200 |
| PostgreSQL | localhost:5432 (postgres/postgres) |

---

## Design Documents

- [DESIGN.md](./DESIGN.md) — Full HLD and LLD
- [ARCHITECTURE.md](./ARCHITECTURE.md) — Architecture diagrams
- [CLAUDE.md](./CLAUDE.md) — Session context for Claude Code
