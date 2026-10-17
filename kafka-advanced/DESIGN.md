# kafka-advanced — HLD & LLD Design Document

---

## HIGH-LEVEL DESIGN (HLD)

### System Overview

kafka-advanced is a distributed event-driven system that processes e-commerce orders
across multiple microservices using Apache Kafka as the message backbone.
It demonstrates production-grade Kafka patterns: idempotency, retry, dead letter topics,
Kafka Streams, schema registry, and transactions.

---

### HLD Architecture Diagram

```
                          ┌──────────────────────────────────────────────────────────┐
                          │                    CLIENT LAYER                          │
                          └──────────────────────────────────────────────────────────┘
                                              │
                                    POST /api/v1/orders
                                              │
                          ┌───────────────────▼──────────────────────┐
                          │             order-service                 │
                          │               Port: 8081                  │
                          │                                           │
                          │  ┌─────────────────────────────────────┐ │
                          │  │  Idempotent Producer                 │ │
                          │  │  Partition Key = customerId          │ │
                          │  └─────────────────────────────────────┘ │
                          └───────────────────┬──────────────────────┘
                                              │
                                    publishes OrderEventDTO
                                    key = customerId
                                              │
                          ┌───────────────────▼──────────────────────┐
                          │          Apache Kafka Broker              │
                          │                                           │
                          │   Topic: order-events (3 partitions)      │
                          │   ┌──────────┬──────────┬──────────┐     │
                          │   │Partition0│Partition1│Partition2│     │
                          │   │CUST-001  │CUST-002  │CUST-003  │     │
                          │   └──────────┴──────────┴──────────┘     │
                          └──────┬───────────┬───────────┬───────────┘
                                 │           │           │
               ┌─────────────────┘     ┌─────┘     ┌────┘
               │                       │            │
               ▼                       ▼            ▼
  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
  │  payment-service   │  │  shipping-service  │  │   audit-service    │
  │    Port: 8082      │  │    Port: 8083      │  │    Port: 8084      │
  │                    │  │                    │  │                    │
  │  group:            │  │  group:            │  │  Kafka Streams     │
  │  payment-group     │  │  shipping-group    │  │  Real-time counts  │
  │                    │  │                    │  │  Aggregations      │
  │  Retry 3x + DLT    │  │  Global Error      │  │                    │
  │  Saves to DB       │  │  Handler           │  │                    │
  └────────┬───────────┘  └────────────────────┘  └────────────────────┘
           │
           │ on failure after 3 retries
           ▼
  ┌─────────────────────────────┐
  │  Kafka: order-events-dlt    │
  └──────────────┬──────────────┘
                 │
                 ▼
  ┌──────────────────────────────┐
  │    dlt-handler-service       │
  │        Port: 8085            │
  │                              │
  │  group: dlt-handler-group    │
  │  Logs + Alerts on failures   │
  └──────────────────────────────┘
```

---

### HLD Components

| Component | Technology | Responsibility |
|---|---|---|
| order-service | Spring Boot + Kafka Producer | Accept REST requests, publish events |
| payment-service | Spring Boot + Kafka Consumer | Process payments, retry on failure, DLT |
| shipping-service | Spring Boot + Kafka Consumer | Process shipments, global error handling |
| audit-service | Spring Boot + Kafka Streams | Real-time aggregation and reporting |
| dlt-handler-service | Spring Boot + Kafka Consumer | Handle dead letter messages, alert ops |
| base-domain | Plain Maven Library | Shared DTOs and enums |
| Apache Kafka | Confluent cp-kafka:7.6.0 | Message broker, topic management |
| PostgreSQL | postgres:15-alpine | Payment records persistence |
| Kafka UI | provectuslabs/kafka-ui | Topic and consumer group monitoring |
| Zipkin | openzipkin/zipkin:3.4 | Distributed tracing |
| ELK Stack | Elastic 8.15.3 | Centralised log management |

---

### HLD Data Flow

```
Step 1:  Client sends POST /api/v1/orders with OrderDTO
Step 2:  order-service sets orderId, status=PENDING, wraps in OrderEventDTO
Step 3:  Idempotent producer sends to order-events topic
         Key = customerId → same customer always on same partition
Step 4:  THREE consumers receive the SAME event independently:
         ├── payment-service  → processes payment → saves to PostgreSQL
         ├── shipping-service → schedules shipment
         └── audit-service    → updates real-time order counters (Kafka Streams)
Step 5:  If payment-service fails:
         ├── Retry 1 after 1s
         ├── Retry 2 after 2s
         ├── Retry 3 after 4s
         └── After all retries → message moves to order-events-dlt
Step 6:  dlt-handler-service consumes order-events-dlt
         └── Logs error, sends alert, triggers manual intervention
```

---

### HLD Non-Functional Requirements

| Concern | Solution |
|---|---|
| Duplicate messages | Idempotent producer (enable.idempotence=true) |
| Message ordering | Partition by customerId — order guaranteed per customer |
| Fault tolerance | Retry with exponential backoff |
| Failed message handling | Dead Letter Topic |
| Observability | ELK Stack + Zipkin tracing |
| Consumer visibility | Kafka UI — lag monitoring |
| Data persistence | PostgreSQL for payment records |

---
---

## LOW-LEVEL DESIGN (LLD)

### Service: order-service

#### Class Diagram
```
OrderController
├── POST /api/v1/orders (@RequestBody OrderDTO)
│   ├── sets orderId    = UUID.randomUUID()
│   ├── sets status     = OrderStatus.PENDING
│   ├── sets createdAt  = LocalDateTime.now()
│   ├── builds OrderEventDTO (eventId, eventType=ORDER_PLACED, order, createdAt)
│   └── calls orderKafkaProducer.sendMessage(orderEventDTO)

OrderKafkaProducer
├── KafkaTemplate<String, OrderEventDTO>
├── sendMessage(OrderEventDTO)
│   ├── key = orderEventDTO.getOrder().getCustomerId()  ← partition key
│   ├── kafkaTemplate.send(topic, key, payload)
│   └── CompletableFuture.whenComplete() → logs partition + offset on success

KafkaTopicConfig
└── @Bean NewTopic orderEventsTopic()
    └── TopicBuilder.name("order-events").partitions(3).replicas(1).build()
```

#### Idempotent Producer Config
```yaml
enable.idempotence: true         # No duplicates on retry
acks: all                        # All replicas confirm
retries: 3                       # Retry 3 times
max.in.flight.requests: 5        # Max 5 unacked requests (safe with idempotence)
```

---

### Service: payment-service

#### Class Diagram
```
PaymentKafkaConsumer
├── @RetryableTopic(attempts=4, backoff=1s*2x)
├── @KafkaListener(topic=order-events, group=payment-service-group)
├── consume(OrderEventDTO, partition, offset)
│   ├── logs all event + order fields
│   ├── calls processPayment(orderEventDTO)
│   │   └── throws RuntimeException if price==1 (simulate failure)
│   ├── builds PaymentRecord entity
│   └── paymentRecordRepository.save(record)
│
└── @DltHandler
    └── handleDlt(OrderEventDTO, topic, partition, offset)
        └── logs FAILED message details + required actions

PaymentRecord (JPA Entity)
├── @Table("payment_records")
├── id (auto-generated)
├── orderId, customerId, productName
├── amount (BigDecimal)
├── paymentStatus (SUCCESS / FAILED)
├── eventType
└── processedAt (LocalDateTime)

PaymentRecordRepository
└── JpaRepository<PaymentRecord, Long>
```

#### Retry Flow
```
attempt 1 → consume() → exception thrown
              └── wait 1000ms
attempt 2 → consume() → exception thrown
              └── wait 2000ms
attempt 3 → consume() → exception thrown
              └── wait 4000ms
attempt 4 → consume() → exception thrown
              └── message sent to order-events-dlt
              └── @DltHandler handleDlt() called
```

#### Retry Topics Auto-Created
```
order-events           ← original
order-events-retry-0   ← after attempt 1 fails
order-events-retry-1   ← after attempt 2 fails
order-events-retry-2   ← after attempt 3 fails
order-events-dlt       ← after attempt 4 fails
```

---

### Service: shipping-service

#### Class Diagram
```
KafkaErrorHandlerConfig
└── @Bean DefaultErrorHandler
    ├── FixedBackOff(interval=1000ms, maxAttempts=3)
    ├── exhausted handler → logs error, triggers alert
    └── addNotRetryableExceptions(IllegalArgumentException, NullPointerException)
        └── these exceptions skip retry — fail fast

ShippingKafkaConsumer
└── @KafkaListener(topic=order-events, group=shipping-service-group)
    └── consume(OrderEventDTO, partition, offset)
        ├── logs all event + order fields
        └── processes shipment (label creation, scheduling)
```

#### Global Error Handler vs @RetryableTopic

| | Global Error Handler | @RetryableTopic |
|---|---|---|
| Where | shipping-service | payment-service |
| Retry storage | in-memory | separate Kafka topics |
| Survives service restart | ❌ No | ✅ Yes |
| DLT support | ❌ Manual | ✅ Auto |
| Best for | lightweight retry | critical business flows |

---

### Service: dlt-handler-service

#### Class Diagram
```
DltKafkaConsumer
└── @KafkaListener(topic=order-events-dlt, group=dlt-handler-group)
    └── consumeFromDlt(OrderEventDTO, topic, partition, offset)
        ├── logs full failed message details at ERROR level
        └── action items:
            ├── send alert email to ops team
            ├── create Jira / PagerDuty ticket
            ├── send failure notification to customer
            └── store in DB for audit trail
```

---

### Service: audit-service (Phase 2 — Kafka Streams)

#### Planned Class Diagram
```
KafkaStreamsConfig
└── @Bean KStream<String, OrderEventDTO>
    ├── stream from order-events topic
    ├── groupByKey (customerId)
    ├── count() → KTable<String, Long>  ← orders per customer
    └── toStream().to(order-counts topic)

OrderCountConsumer
└── @KafkaListener(topic=order-counts)
    └── logs real-time order count per customer
```

---

### Base Domain — Shared Classes

```
com.basedomain.dto
├── OrderDTO
│   ├── orderId, customerId, productId, productName
│   ├── quantity (Integer), price (BigDecimal)
│   ├── status (OrderStatus)
│   └── createdAt (LocalDateTime)
│
├── OrderEventDTO
│   ├── eventId, eventType (OrderEventType)
│   ├── order (OrderDTO)
│   ├── message, status (String)
│   └── createdAt (LocalDateTime)
│
└── PaymentEventDTO
    ├── paymentId, orderId, customerId
    ├── amount (BigDecimal)
    ├── status (PaymentStatus)
    ├── message
    └── createdAt (LocalDateTime)

com.basedomain.enums
├── OrderStatus:    PENDING, CONFIRMED, PROCESSING, SHIPPED, DELIVERED, CANCELLED, FAILED
├── OrderEventType: ORDER_PLACED, ORDER_CONFIRMED, ORDER_CANCELLED, ORDER_FAILED
└── PaymentStatus:  PENDING, SUCCESS, FAILED, REFUNDED
```

---

### Kafka Topics — Full Reference

| Topic | Partitions | Key | Producer | Consumers | Purpose |
|---|---|---|---|---|---|
| order-events | 3 | customerId | order-service | payment, shipping, audit | Main event stream |
| order-events-retry-0 | 1 | — | Spring auto | payment-service | Retry attempt 1 |
| order-events-retry-1 | 1 | — | Spring auto | payment-service | Retry attempt 2 |
| order-events-retry-2 | 1 | — | Spring auto | payment-service | Retry attempt 3 |
| order-events-dlt | 1 | — | Spring auto | dlt-handler-service | Failed messages |

---

### Database Design — PostgreSQL

#### payment_records table
```sql
CREATE TABLE payment_records (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(255),
    customer_id     VARCHAR(255),
    product_name    VARCHAR(255),
    amount          NUMERIC(19,2),
    payment_status  VARCHAR(50),    -- SUCCESS or FAILED
    event_type      VARCHAR(50),
    processed_at    TIMESTAMP
);
```

---

### Port Reference

| Service | Port |
|---|---|
| order-service | 8081 |
| payment-service | 8082 |
| shipping-service | 8083 |
| audit-service | 8084 |
| dlt-handler-service | 8085 |
| Kafka | 9092 |
| Zookeeper | 2181 |
| Kafka UI | 8080 |
| PostgreSQL | 5432 |
| Zipkin | 9411 |
| Elasticsearch | 9200 |
| Logstash | 5000 |
| Kibana | 5601 |
