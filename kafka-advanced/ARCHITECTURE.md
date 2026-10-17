# kafka-advanced — Architecture

## Overview

Production-grade event-driven system covering all major Kafka concepts across three phases:
Idempotent Producer, Partitioning, Retry, DLT, Kafka Streams, Consumer Lag Monitoring,
Schema Registry + Avro, Kafka Transactions, Kafka Connect, and SSL/SASL Security.

---

## Full System Architecture

```
[REST Client]
      │
      │  POST /api/v1/orders
      ▼
┌──────────────────────────────────────┐
│           order-service              │  Port: 8081
│                                      │
│  - Idempotent Producer               │
│  - Partition by customerId           │
│  - Transactional Producer (Phase 3)  │
│  - KafkaTopicConfig (3 partitions)   │
└──────────────────┬───────────────────┘
                   │  publishes OrderEventDTO
                   │  key = customerId
                   ▼
┌──────────────────────────────────────────────────┐
│           Apache Kafka Broker                    │
│                                                  │
│   Topic: order-events (3 partitions)             │
│   ┌──────────────┬──────────────┬─────────────┐  │
│   │  Partition 0 │  Partition 1 │ Partition 2 │  │
│   │  CUST-001    │  CUST-002    │  CUST-003   │  │
│   └──────────────┴──────────────┴─────────────┘  │
│                                                  │
│   Topic: order-confirmed-events (3 partitions)   │
│   (written atomically with order-events)         │
│                                                  │
│   Topic: avro-order-events (1 partition)         │
│   Topic: order-events-dlt (1 partition)          │
└────┬──────────────┬──────────────────────────────┘
     │              │              │
     ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐
│ payment-    │ │ shipping-   │ │    audit-service     │
│ service     │ │ service     │ │       Port: 8084     │
│ Port: 8082  │ │ Port: 8083  │ │                      │
│             │ │             │ │  @EnableKafkaStreams  │
│ @Retryable  │ │ Default     │ │  KStream → groupBy   │
│ Topic 3x    │ │ ErrorHandler│ │  count() → KTable    │
│ DLT auto    │ │ 3 retries   │ │  aggregate() revenue │
│ → PostgreSQL│ │             │ │  → derived topics    │
│ Consumer    │ │ Consumer    │ │                      │
│ Lag API     │ │ Lag API     │ │                      │
│ read_       │ │             │ │                      │
│ committed   │ │             │ │                      │
└──────┬──────┘ └─────────────┘ └─────────────────────┘
       │
       │  after 3 retries fail
       ▼
┌──────────────────────┐
│  order-events-dlt    │
└──────────┬───────────┘
           ▼
┌──────────────────────────┐
│   dlt-handler-service    │  Port: 8085
│   group: dlt-handler-    │
│   group                  │
│   Logs + Alerts          │
│   Consumer Lag API       │
└──────────────────────────┘

[Avro Path — separate topic]
┌──────────────────────────┐     registers schema     ┌──────────────────────┐
│    avro-service          │ ──────────────────────▶  │  Schema Registry     │
│    Port: 8086            │ ◀──────────────────────  │  Port: 8091          │
│                          │     returns schema ID     └──────────────────────┘
│  KafkaAvroSerializer     │
│  KafkaAvroDeserializer   │
│  SpecificRecord          │
│  Producer + Consumer     │
└──────────────────────────┘
       │ sends binary Avro
       ▼
[ avro-order-events topic ]

[Kafka Connect — DB sink]
[ order-events topic ] ──▶ kafka-connect (8083) ──▶ PostgreSQL order_events_sink
                           JDBC Sink Connector
                           upsert mode, auto.create
```

---

## Services

| Service | Port | Kafka Role | Concepts |
|---|---|---|---|
| `order-service` | 8081 | Producer | Idempotent, Partition by key, Transactions |
| `payment-service` | 8082 | Consumer | Retry 3x, DLT, DB save, Consumer Lag, read_committed |
| `shipping-service` | 8083 | Consumer | Global Error Handler, Consumer Lag |
| `audit-service` | 8084 | Streams | Kafka Streams, KStream/KTable, aggregation |
| `dlt-handler-service` | 8085 | Consumer | DLT handling, Consumer Lag |
| `avro-service` | 8086 | Producer + Consumer | Schema Registry, Avro, SpecificRecord |
| `base-domain` | — | Shared lib | DTOs, Enums |
| `kafka-connect` | 8083* | Connector | JDBC Sink, PostgreSQL sink |
| `security-config` | — | Config | SASL/PLAIN, broker JAAS |

*Kafka Connect REST API — Docker only.

---

## Kafka Topics

| Topic | Partitions | Key | Producer | Consumers | Purpose |
|---|---|---|---|---|---|
| `order-events` | 3 | customerId | order-service | payment, shipping, audit, kafka-connect | Main event stream |
| `order-confirmed-events` | 3 | customerId | order-service (TX) | — | Atomic transaction partner topic |
| `order-events-retry-0` | 1 | — | Spring auto | payment-service | Retry 1 |
| `order-events-retry-1` | 1 | — | Spring auto | payment-service | Retry 2 |
| `order-events-retry-2` | 1 | — | Spring auto | payment-service | Retry 3 |
| `order-events-dlt` | 1 | — | Spring auto | dlt-handler-service | Failed messages |
| `avro-order-events` | 1 | customerId | avro-service | avro-service | Avro binary messages |
| `order-count-per-customer` | 1 | customerId | audit-service (Streams) | — | Running order count per customer |
| `revenue-per-product` | 1 | productName | audit-service (Streams) | — | Running revenue per product |
| `connect-configs` | 1 | — | Kafka Connect | Kafka Connect | Connect internal config storage |
| `connect-offsets` | 25 | — | Kafka Connect | Kafka Connect | Connect consumer offset tracking |
| `connect-status` | 5 | — | Kafka Connect | Kafka Connect | Connector + task health status |

---

## Concepts Per Phase

### Phase 1 — Practical Kafka ✅

| Concept | Service | Key Code |
|---|---|---|
| Idempotent Producer | order-service | `enable.idempotence: true`, `acks: all` |
| Partition by Key | order-service | `kafkaTemplate.send(topic, customerId, event)` |
| Retry + Backoff | payment-service | `@RetryableTopic(attempts=4, backoff=1s×2)` |
| Dead Letter Topic | payment-service | `@DltHandler`, auto-created `-dlt` topic |
| DLT Consumer | dlt-handler-service | `@KafkaListener(topics="order-events-dlt")` |
| Global Error Handler | shipping-service | `DefaultErrorHandler` + `FixedBackOff(1000, 3)` |

### Phase 2 — Intermediate Kafka ✅

| Concept | Service | Key Code |
|---|---|---|
| Kafka Streams | audit-service | `@EnableKafkaStreams`, `KStream.groupByKey().count()`, `aggregate()` |
| Consumer Lag Monitoring | payment, shipping, dlt-handler | `AdminClient` → `GET /api/consumer-lag` |
| Schema Registry + Avro | avro-service | `order-event.avsc` → generated `OrderEvent.java`, `KafkaAvroSerializer` |

### Phase 3 — Production Kafka ✅

| Concept | Service | Key Code |
|---|---|---|
| Kafka Transactions | order-service | `transaction-id-prefix: order-tx-`, `executeInTransaction()` |
| read_committed | payment-service | `isolation.level: read_committed` |
| Kafka Connect | kafka-connect | JDBC Sink Connector, `order-events` → `order_events_sink` table |
| SSL/SASL | security-config | `kafka_server_jaas.conf`, `SASL_PLAINTEXT` listeners |

---

## Infrastructure

| Tool | Port | Purpose |
|---|---|---|
| Kafka | 9092 | Message broker |
| Zookeeper | 2181 | Kafka coordinator |
| Schema Registry | 8091 | Avro schema storage + versioning |
| Kafka UI | 8080 | Topics, consumer groups, Schema Registry browser |
| Kafka Connect | 8083 | Sink/source connector runtime + REST API |
| PostgreSQL | 5432 | payment records + order_events_sink table |
| Zipkin | 9411 | Distributed tracing |
| Elasticsearch | 9200 | Log storage |
| Logstash | 5000 | Log pipeline (TCP JSON → Elasticsearch) |
| Kibana | 5601 | Log dashboard |

---

## Data Flow — Normal Order

```
1.  Client → POST /api/v1/orders
2.  order-service creates OrderEventDTO
    - orderId     = UUID
    - customerId  = from request (partition key)
    - eventType   = ORDER_PLACED
    - status      = PENDING
3.  Idempotent producer sends to order-events
    - Key = customerId → same partition every time
    - Kafka assigns sequence number → deduplicates retries
4.  THREE consumers receive the same event independently:
    ├── payment-service (payment-service-group)
    │     - processes payment, saves PaymentRecord to PostgreSQL
    │     - on failure: retries 1s → 2s → 4s → DLT
    ├── shipping-service (shipping-service-group)
    │     - schedules shipment
    │     - global error handler catches exceptions
    └── audit-service (Kafka Streams)
          - counts orders per customerId in real-time
          - aggregates revenue per productName
5.  Kafka Connect reads order-events
    - JDBC Sink Connector writes each message to order_events_sink table in PostgreSQL
6.  If payment fails after 3 retries:
    - message moves to order-events-dlt
    - dlt-handler-service logs + alerts
```

## Data Flow — Transactional Order

```
1.  Client → POST /api/v1/orders/transactional
2.  order-service calls kafkaTemplate.executeInTransaction():
    a. send ORDER_PLACED  → order-events topic          (not yet visible)
    b. send ORDER_CONFIRMED → order-confirmed-events    (not yet visible)
    c. commitTransaction()
    → BOTH messages become visible simultaneously
    → payment-service (read_committed) sees the messages only after commit
```

## Data Flow — Avro Order

```
1.  Client → POST /api/avro/orders
2.  avro-service builds OrderEvent (generated Avro class)
3.  KafkaAvroSerializer:
    a. registers schema in Schema Registry (or fetches ID if known)
    b. serializes: [0x00][schema ID (4 bytes)][avro binary]
4.  Message sent to avro-order-events topic
5.  avro-service consumer:
    a. reads schema ID from first 5 bytes
    b. fetches schema from Schema Registry
    c. deserializes → typed OrderEvent object
```

---

## Port Reference

| Service | Port |
|---|---|
| order-service | 8081 |
| payment-service | 8082 |
| shipping-service | 8083 |
| audit-service | 8084 |
| dlt-handler-service | 8085 |
| avro-service | 8086 |
| Kafka | 9092 |
| Zookeeper | 2181 |
| Schema Registry | 8091 |
| Kafka UI | 8080 |
| Kafka Connect | 8083 (Docker) |
| PostgreSQL | 5432 |
| Zipkin | 9411 |
| Elasticsearch | 9200 |
| Logstash | 5000 |
| Kibana | 5601 |
