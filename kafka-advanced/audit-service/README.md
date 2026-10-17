# Audit Service — Kafka Streams

## What This Service Does

The audit-service uses **Kafka Streams** to process the `order-events` topic in real time and produce two derived streams:

1. **Order count per customer** → written to `order-count-per-customer` topic
2. **Revenue per product** → written to `revenue-per-product` topic

This is not a consumer. It is a stream processor — it reads, transforms, aggregates, and writes results continuously.

---

## Core Concept: Kafka Streams

### What is Kafka Streams?

Kafka Streams is a **client-side library** (not a separate cluster) that lets you process data flowing through Kafka topics using a high-level DSL. You run it inside your application — no extra infrastructure needed.

```
order-events topic
       │
       ▼
  Kafka Streams (inside audit-service JVM)
       │
       ├──▶ order-count-per-customer topic
       │
       └──▶ revenue-per-product topic
```

### KStream vs KTable

| Concept | Description | Analogy |
|---------|-------------|---------|
| **KStream** | Unbounded, append-only stream of records | A river that keeps flowing |
| **KTable** | Latest value per key — like a materialised view | A database table with live updates |

When you call `.count()` or `.aggregate()`, you get a **KTable** — the latest aggregated result per key.

### Key Operations Used

```java
// Read a topic as a KStream
KStream<String, String> stream = streamsBuilder.stream("order-events");

// Group by the existing key (customerId)
KGroupedStream<String, String> grouped = stream.groupByKey();

// Count records per key → KTable
KTable<String, Long> countTable = grouped.count(Materialized.as("order-count-store"));

// Re-key to a different field before aggregating
stream
    .selectKey((k, v) -> extractProductName(v))   // change key to productName
    .groupByKey()
    .aggregate(
        () -> "0",                                  // initializer
        (key, value, current) -> add(current, value), // accumulator
        Materialized.as("revenue-store")
    );
```

### State Stores

Both aggregations use a **state store** (an embedded RocksDB or in-memory store):

- `order-count-store` — holds running order count per customerId
- `revenue-store` — holds running revenue per productName

These state stores let Kafka Streams resume from exactly where it left off after a restart.

---

## Why @EnableKafkaStreams?

`@EnableKafkaStreams` tells Spring to:
1. Look for a bean named `defaultKafkaStreamsConfig`
2. Automatically inject a `StreamsBuilder` into your `@Bean` methods
3. Start the Kafka Streams processing loop on application startup

Without it, the `StreamsBuilder` is never processed and your stream topology never starts.

---

## Application Config

| Property | Value |
|----------|-------|
| Port | 8084 |
| Input topic | `order-events` |
| Output topic 1 | `order-count-per-customer` |
| Output topic 2 | `revenue-per-product` |
| Stream app ID | `audit-service-streams` |
| Commit interval | 1000ms |

---

## Key Files

| File | Purpose |
|------|---------|
| `AuditServiceApplication.java` | Entry point — `@EnableKafkaStreams` activates stream processing |
| `config/KafkaStreamsConfig.java` | Defines `KafkaStreamsConfiguration` bean (app ID, serdes, bootstrap servers) |
| `streams/OrderAuditStream.java` | Defines two Kafka Streams topologies |

---

## Stream Topology Diagram

```
order-events (input)
       │
       ├──── groupByKey (customerId) ──── count() ──── KTable<customerId, Long>
       │                                                      │
       │                                               toStream()
       │                                                      │
       │                                         order-count-per-customer (output)
       │
       └──── mapValues (extract productName:price)
               │
               selectKey (productName)
               │
               groupByKey
               │
               aggregate (sum prices) ──── KTable<productName, BigDecimal>
                                                  │
                                           toStream()
                                                  │
                                        revenue-per-product (output)
```

---

## How to Run

### Prerequisites

```bash
# Start Kafka infrastructure
cd /Users/mohammadmateen/kafka/kafka-advanced
docker-compose up -d zookeeper kafka kafka-ui
```

### Start the Service

```bash
cd audit-service
mvn spring-boot:run
```

### Verify Streams Are Running

1. **Produce an order event** — use order-service POST `/api/orders`
2. **Check output topics** in Kafka UI at `http://localhost:8080`
   - Browse topic `order-count-per-customer`
   - Browse topic `revenue-per-product`

---

## Sample Log Output

```
Kafka Streams: listening to topic 'order-events'
Stream received — customerId: CUST001 | payload: {"eventId":"...","order":{...}}
Order count update — Customer: CUST001 | Total orders: 1
Revenue update — Product: Laptop | +1299.99 | Total: 1299.99
Revenue snapshot — Product: Laptop | Total revenue: 1299.99
```

---

## Difference: Kafka Streams vs @KafkaListener

| Feature | `@KafkaListener` | Kafka Streams |
|---------|-----------------|---------------|
| Use case | Consume and act | Transform and aggregate |
| Output | Side effects (DB save, email) | New Kafka topics |
| Stateful aggregation | Manual (use Redis/DB) | Built-in (state stores) |
| Fault tolerance | At-least-once by default | Exactly-once available |
| Scaling | Add consumer instances | Add stream instances (same app ID) |

Use `@KafkaListener` when you need to react to events (save to DB, send notifications).
Use Kafka Streams when you need to transform, enrich, or aggregate event data into derived streams.

---

## Phase in Project

This service is part of **Phase 2 — Intermediate Kafka**:

- Phase 1 ✅ — Idempotent Producer, Partitioning, Retry + DLT, Global Error Handler
- **Phase 2 (this)** — Kafka Streams, Consumer Lag Monitoring, Schema Registry + Avro
- Phase 3 — Kafka Transactions, Kafka Connect, SSL/SASL
