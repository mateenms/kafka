# kafka-advanced

Production-grade Apache Kafka microservices covering all major advanced concepts across three phases:
Idempotent Producer, Partitioning, Retry, DLT, Kafka Streams, Schema Registry + Avro, Consumer Lag,
Kafka Transactions, Kafka Connect, and SSL/SASL Security.

---

## Services

| Service | Port | Concept | Phase |
|---|---|---|---|
| [base-domain](./base-domain/) | — | Shared DTOs & Enums | ✅ Phase 1 |
| [order-service](./order-service/) | 8081 | Idempotent Producer, Partition by key, Transactions | ✅ Phase 1 + 3 |
| [payment-service](./payment-service/) | 8082 | Retry 3x + DLT + PostgreSQL + Consumer Lag | ✅ Phase 1 + 2 |
| [shipping-service](./shipping-service/) | 8083 | Global Error Handler + Consumer Lag | ✅ Phase 1 + 2 |
| [dlt-handler-service](./dlt-handler-service/) | 8085 | DLT Consumer + Consumer Lag | ✅ Phase 1 + 2 |
| [audit-service](./audit-service/) | 8084 | Kafka Streams — real-time aggregation | ✅ Phase 2 |
| [avro-service](./avro-service/) | 8086 | Schema Registry + Avro serialization | ✅ Phase 2 |
| [kafka-connect](./kafka-connect/) | 8083* | JDBC Sink Connector — topic → PostgreSQL | ✅ Phase 3 |
| [security-config](./security-config/) | — | SASL/PLAIN authentication | ✅ Phase 3 |

*Kafka Connect REST API port. Does not conflict with shipping-service (8083) since Connect runs in Docker.

---

## Architecture

```
[REST Client]
      │
      │  POST /api/v1/orders
      ▼
order-service (8081)
Idempotent Producer
Partition by customerId
Transactional Producer (Phase 3)
      │
      ▼
[ Kafka: order-events — 3 partitions ]
      │
      ├──────────────────────────────┬──────────────────────────────┐
      ▼                              ▼                              ▼
payment-service (8082)    shipping-service (8083)       audit-service (8084)
Retry 3x + DLT            Global Error Handler          Kafka Streams
Save to PostgreSQL         Consumer Lag API              KStream aggregation
Consumer Lag API                                         Order count / Revenue
read_committed                                           → derived topics
      │
      │ on failure after 3 retries
      ▼
[ Kafka: order-events-dlt ]
      │
      ▼
dlt-handler-service (8085)
Log + Alert + Manual intervention
Consumer Lag API

[ Kafka: avro-order-events ]
      │
      ▼
avro-service (8086)        ← produces AND consumes Avro binary messages
Schema Registry (8091)     ← stores + versions Avro schemas

[ Kafka: order-events ]
      │
      ▼
kafka-connect (8083)       ← JDBC Sink: writes messages to PostgreSQL table
PostgreSQL → order_events_sink table
```

---

## Kafka Concepts Covered

| # | Concept | Service | Phase |
|---|---|---|---|
| 1 | Idempotent Producer | order-service | Phase 1 |
| 2 | Partition by Key | order-service | Phase 1 |
| 3 | Retry + Backoff | payment-service | Phase 1 |
| 4 | Dead Letter Topic | payment-service | Phase 1 |
| 5 | DLT Consumer | dlt-handler-service | Phase 1 |
| 6 | Global Error Handler | shipping-service | Phase 1 |
| 7 | Kafka Streams | audit-service | Phase 2 |
| 8 | Consumer Lag Monitoring | payment, shipping, dlt-handler | Phase 2 |
| 9 | Schema Registry + Avro | avro-service | Phase 2 |
| 10 | Kafka Transactions | order-service | Phase 3 |
| 11 | Kafka Connect | kafka-connect | Phase 3 |
| 12 | SSL/SASL Security | security-config | Phase 3 |

---

## Quick Start

### Step 1 — Start infrastructure

```bash
cd /Users/mohammadmateen/kafka/kafka-advanced
docker compose up -d
```

Starts: Kafka, Zookeeper, Schema Registry, Kafka UI, Kafka Connect, PostgreSQL, Elasticsearch, Logstash, Kibana, Zipkin

### Step 2 — Build shared library

```bash
mvn clean install -q -f base-domain/pom.xml
```

### Step 3 — Build all services

```bash
for svc in order-service payment-service shipping-service dlt-handler-service audit-service avro-service; do
  mvn clean install -DskipTests -q -f $svc/pom.xml && echo "✅ $svc"
done
```

### Step 4 — Start services (separate terminals)

```bash
java -jar order-service/target/order-service-1.0.0.jar          # 8081
java -jar payment-service/target/payment-service-1.0.0.jar      # 8082
java -jar shipping-service/target/shipping-service-1.0.0.jar    # 8083
java -jar dlt-handler-service/target/dlt-handler-service-1.0.0.jar # 8085
java -jar audit-service/target/audit-service-1.0.0.jar          # 8084
java -jar avro-service/target/avro-service-1.0.0.jar            # 8086
```

### Step 5 — Register Kafka Connect sink connector

```bash
bash kafka-connect/register-connector.sh
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

### Retry + DLT Demo (price = 1 triggers failure)

```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUST-002","productName":"Fail Test","quantity":1,"price":1}'
```

Expected: payment-service retries 3x → message goes to DLT → dlt-handler-service logs error

### Kafka Transactions Demo

```bash
curl -X POST http://localhost:8081/api/v1/orders/transactional \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUST-001","productName":"Laptop","quantity":1,"price":1299.99}'
```

Expected: both `order-events` AND `order-confirmed-events` topics receive messages atomically.

### Avro Demo

```bash
curl -X POST http://localhost:8086/api/avro/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUST-001","productName":"Laptop","quantity":1,"price":"1299.99"}'

# Verify schema registered:
curl http://localhost:8091/subjects
```

### Consumer Lag Check

```bash
curl http://localhost:8082/api/consumer-lag   # payment-service
curl http://localhost:8083/api/consumer-lag   # shipping-service
curl http://localhost:8085/api/consumer-lag   # dlt-handler-service
```

---

## Infrastructure URLs

| Tool | URL | Purpose |
|---|---|---|
| Kafka UI | http://localhost:8080 | Topics, consumer groups, Schema Registry |
| Schema Registry | http://localhost:8091 | Avro schema storage |
| Kafka Connect | http://localhost:8083 | Connector management REST API |
| Kibana | http://localhost:5601 | Log dashboard |
| Zipkin | http://localhost:9411 | Distributed tracing |
| Elasticsearch | http://localhost:9200 | Log storage |
| PostgreSQL | localhost:5432 (postgres/postgres) | payment records, sink table |

---

## Design Documents

- [DESIGN.md](./DESIGN.md) — Full HLD and LLD for all phases
- [ARCHITECTURE.md](./ARCHITECTURE.md) — Architecture diagrams
- [CLAUDE.md](./CLAUDE.md) — Session context for Claude Code
