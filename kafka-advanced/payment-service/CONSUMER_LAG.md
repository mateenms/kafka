# Consumer Lag Monitoring

## What is Consumer Lag?

**Consumer lag** is the difference between the latest available offset in a Kafka partition and the offset the consumer has actually read up to.

```
Partition 0:   [msg0] [msg1] [msg2] [msg3] [msg4]   ← endOffset = 5
                                     ↑
                             currentOffset = 3        ← consumer last committed here

                             Lag = 5 - 3 = 2
```

- **Lag = 0** → consumer is fully caught up
- **Lag > 0** → consumer is behind; messages are waiting to be processed
- **Growing lag** → consumer cannot keep up with the producer rate (alert!)

---

## Why Monitor It?

| Scenario | What lag tells you |
|----------|--------------------|
| Normal operation | Lag stays near 0 |
| Slow consumer | Lag grows steadily |
| Consumer down | Lag grows rapidly |
| Burst of messages | Lag spikes then recovers |
| DLT messages piling up | Lag in dlt-handler group grows |

Monitoring lag lets you detect problems **before** users notice message delays.

---

## How It Works in This Service

We use Kafka's **`AdminClient`** — the admin API for inspecting broker state.

```
AdminClient
    │
    ├── listConsumerGroupOffsets(groupId)
    │       → committed offsets per partition (how far consumer has read)
    │
    └── listOffsets(partitions → OffsetSpec.latest())
            → end offsets per partition (latest message available)

Lag = endOffset − committedOffset
```

### Key Classes

| Class | Purpose |
|-------|---------|
| `KafkaAdminConfig` | Creates the `AdminClient` bean |
| `ConsumerLagService` | Calls AdminClient, calculates lag per partition |
| `ConsumerLagController` | Exposes `GET /api/consumer-lag` |

---

## API Endpoint

```
GET /api/consumer-lag
```

### Sample Response — Caught Up

```json
{
  "groupId": "payment-service-group",
  "status": "HEALTHY",
  "totalLag": 0,
  "partitions": [
    {
      "topic": "order-events",
      "partition": 0,
      "currentOffset": 42,
      "endOffset": 42,
      "lag": 0,
      "status": "CAUGHT_UP"
    }
  ]
}
```

### Sample Response — Lagging

```json
{
  "groupId": "payment-service-group",
  "status": "LAGGING",
  "totalLag": 15,
  "partitions": [
    {
      "topic": "order-events",
      "partition": 0,
      "currentOffset": 27,
      "endOffset": 42,
      "lag": 15,
      "status": "LAGGING"
    }
  ]
}
```

### Response When No Offsets Committed Yet

```json
{
  "groupId": "payment-service-group",
  "status": "NO_COMMITTED_OFFSETS",
  "message": "Consumer group has not committed any offsets yet — service may not have consumed any messages"
}
```

---

## Status Values

| Status | Meaning |
|--------|---------|
| `HEALTHY` | All partitions at lag 0 |
| `LAGGING` | One or more partitions behind |
| `NO_COMMITTED_OFFSETS` | Consumer has never committed (or group doesn't exist yet) |
| `ERROR` | AdminClient call failed (Kafka not reachable) |

---

## How AdminClient Differs from Consumer/Producer

| | Producer | Consumer | AdminClient |
|-|----------|----------|-------------|
| Purpose | Send messages | Read messages | Inspect broker state |
| Requires group ID | No | Yes | No |
| Spring auto-creates | Yes | Yes | **No — must create manually** |
| Used for | Publishing | Processing | Monitoring, management |

AdminClient is a **read-only window into Kafka** — it doesn't consume messages, it just asks the broker questions.

---

## All Services with Lag Monitoring

| Service | Port | Group ID | Topic |
|---------|------|----------|-------|
| payment-service | 8082 | `payment-service-group` | `order-events` |
| shipping-service | 8083 | `shipping-service-group` | `order-events` |
| dlt-handler-service | 8085 | `dlt-handler-group` | `order-events-dlt` |

---

## How to Test

```bash
# 1. Start Kafka
docker-compose up -d zookeeper kafka

# 2. Start a consumer service
mvn spring-boot:run -f payment-service/pom.xml

# 3. Check lag (before any messages — expect NO_COMMITTED_OFFSETS)
curl http://localhost:8082/api/consumer-lag

# 4. Send some orders via order-service
curl -X POST http://localhost:8081/api/orders -H "Content-Type: application/json" \
  -d '{"customerId":"CUST001","productName":"Laptop","price":1299.99,"quantity":1}'

# 5. Check lag again — expect HEALTHY with lag=0 after processing
curl http://localhost:8082/api/consumer-lag
```

---

## Phase in Project

This feature is part of **Phase 2 — Intermediate Kafka**:

- Phase 1 ✅ — Idempotent Producer, Partitioning, Retry + DLT, Global Error Handler
- **Phase 2 (this)** — Kafka Streams, **Consumer Lag Monitoring**, Schema Registry + Avro
- Phase 3 — Kafka Transactions, Kafka Connect, SSL/SASL
