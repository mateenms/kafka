# kafka-advanced — Architecture

## Overview

Production-grade event-driven system covering advanced Kafka concepts:
Retry, Dead Letter Topic, Idempotent Producer, Partitioning, Kafka Streams, Transactions, Schema Registry.

---

## System Architecture

```
[REST Client]
      │
      │  POST /api/v1/orders
      ▼
┌─────────────────────────────┐
│        order-service        │  Port: 8081
│                             │
│  - Idempotent Producer      │
│  - Partition by customerId  │
│  - KafkaTopicConfig         │
└──────────────┬──────────────┘
               │
               │  publishes OrderEventDTO
               ▼
┌──────────────────────────────────────────────────┐
│           Kafka Topic: order-events              │
│           Partitions: 3 (key = customerId)       │
└───────────┬──────────────┬───────────────────────┘
            │              │              │
            ▼              ▼              ▼
┌───────────────┐  ┌───────────────┐  ┌──────────────┐
│payment-service│  │shipping-service  │audit-service │
│  Port: 8082   │  │  Port: 8083   │  │  Port: 8084  │
│               │  │               │  │              │
│ Retry 3x      │  │ Retry 3x      │  │Kafka Streams │
│ DLT on fail   │  │ Error Handler │  │Order counts  │
│ Saves to DB   │  │               │  │Aggregations  │
└───────┬───────┘  └───────────────┘  └──────────────┘
        │
        │  failed after 3 retries
        ▼
┌──────────────────────────────────────┐
│   Kafka Topic: order-events-dlt      │
└──────────────────┬───────────────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │  dlt-handler-service │  Port: 8085
        │                      │
        │  Logs failed events  │
        │  Sends alerts        │
        └──────────────────────┘
```

---

## Services

| Service | Port | Kafka Role | Concept Covered |
|---|---|---|---|
| `order-service` | 8081 | Producer | Idempotent Producer, Partitioning by key |
| `payment-service` | 8082 | Consumer | Retry 3x, Dead Letter Topic, DB save |
| `shipping-service` | 8083 | Consumer | Global Error Handler, Retry |
| `audit-service` | 8084 | Streams | Kafka Streams, real-time aggregation |
| `dlt-handler-service` | 8085 | Consumer | DLT handling, failure alerting |
| `base-domain` | — | Shared lib | DTOs, Enums |

---

## Kafka Topics

| Topic | Partitions | Key | Producer | Consumers |
|---|---|---|---|---|
| `order-events` | 3 | customerId | order-service | payment, shipping, audit |
| `order-events-dlt` | 1 | — | Spring (auto) | dlt-handler-service |

---

## Concepts Per Phase

### Phase 1 — Practical
| Concept | Service | How |
|---|---|---|
| Idempotent Producer | order-service | `enable.idempotence=true` |
| Partition by key | order-service | `customerId` as message key |
| Retry (3x + backoff) | payment-service | `@RetryableTopic(attempts="3")` |
| Dead Letter Topic | payment-service | auto-created `order-events-dlt` |
| Global Error Handler | shipping-service | `DefaultErrorHandler` bean |

### Phase 2 — Intermediate
| Concept | Service | How |
|---|---|---|
| Kafka Streams | audit-service | `KStream`, `KTable`, windowed aggregation |
| Consumer Lag | all | Actuator + Kafka UI |
| Schema Registry + Avro | all | Confluent Schema Registry |

### Phase 3 — Production
| Concept | Service | How |
|---|---|---|
| Transactions | order + payment | `@Transactional` + Kafka TX manager |
| Kafka Connect | — | JDBC sink connector |
| SSL/SASL | broker + all | Keystore + truststore config |

---

## Infrastructure

| Tool | Port | Purpose |
|---|---|---|
| Kafka | 9092 | Message broker |
| Zookeeper | 2181 | Kafka coordinator |
| Kafka UI | 8080 | Topic + consumer group browser |
| PostgreSQL | 5432 | payment-service persistence |
| Zipkin | 9411 | Distributed tracing |
| Elasticsearch | 9200 | Log storage |
| Logstash | 5000 | Log pipeline |
| Kibana | 5601 | Log dashboard |

---

## Data Flow — Order Placed

```
1. Client POST /api/v1/orders
2. order-service creates OrderEventDTO
   - orderId    = UUID
   - customerId = from request (used as partition key)
   - eventType  = ORDER_PLACED
   - status     = PENDING
3. Idempotent producer sends to order-events topic
   - Key = customerId → same customer always hits same partition
   - Guarantees message order per customer
4. payment-service consumes (group: payment-service-group)
   - Attempts to process payment
   - On failure: retries 3 times with 1s backoff
   - After 3 failures: message sent to order-events-dlt
   - On success: saves PaymentEvent to PostgreSQL
5. shipping-service consumes (group: shipping-service-group)
   - Processes shipping
   - Global error handler catches any exception
6. audit-service consumes via Kafka Streams
   - Counts orders per customer in real-time
   - Aggregates totals per product
7. dlt-handler-service consumes order-events-dlt
   - Logs the failed message with full details
   - Could trigger alert/notification
```
