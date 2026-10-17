# kafka-advanced — HLD & LLD Design Document

---

## HIGH-LEVEL DESIGN (HLD)

### System Overview

kafka-advanced is a distributed event-driven system that processes e-commerce orders
across multiple microservices using Apache Kafka as the message backbone.
It demonstrates production-grade Kafka patterns across three phases:
idempotency, retry, dead letter topics, Kafka Streams, consumer lag monitoring,
schema registry + Avro, Kafka transactions, Kafka Connect, and SSL/SASL security.

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

#### Class Diagram
```
AuditServiceApplication
└── @EnableKafkaStreams  ← activates Kafka Streams engine in Spring

KafkaStreamsConfig
└── @Bean(DEFAULT_STREAMS_CONFIG_BEAN_NAME) KafkaStreamsConfiguration
    ├── APPLICATION_ID_CONFIG = "audit-service-streams"
    ├── BOOTSTRAP_SERVERS_CONFIG
    ├── DEFAULT_KEY_SERDE = Serdes.String()
    ├── DEFAULT_VALUE_SERDE = Serdes.String()
    └── COMMIT_INTERVAL_MS_CONFIG = 1000

OrderAuditStream
└── @Bean KStream<String, String> orderAuditStream(StreamsBuilder)
    │
    ├── Stream 1: Order count per customer
    │   KStream.stream(order-events)
    │     .peek(log received)
    │     .groupByKey(customerId)
    │     .count(Materialized.as("order-count-store"))  → KTable<String, Long>
    │     .toStream()
    │     .mapValues("Total orders: " + count)
    │     .to(order-count-per-customer)
    │
    └── Stream 2: Revenue per product
        KStream.stream(order-events)
          .mapValues(json → "productName:price")
          .filter(value != null)
          .selectKey(productName)
          .groupByKey()
          .aggregate("0", (k, price, total) → total.add(price),
                     Materialized.as("revenue-store"))
          .toStream()
          .mapValues("Total revenue: " + amount)
          .to(revenue-per-product)
```

#### State Stores
```
order-count-store  → RocksDB (persistent), keyed by customerId
revenue-store      → RocksDB (persistent), keyed by productName
Both stores survive service restart — Kafka Streams replays from last checkpoint
```

#### Output Topics
```
order-count-per-customer  ← key=customerId, value="Total orders: 5"
revenue-per-product       ← key=productName, value="Total revenue: 6499.95"
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

---

### Service: Consumer Lag Monitoring (Phase 2 — payment, shipping, dlt-handler)

#### Class Diagram (same pattern in all three services)
```
KafkaAdminConfig
└── @Bean AdminClient
    └── AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))

ConsumerLagService
└── getConsumerLag()
    ├── adminClient.listConsumerGroupOffsets(groupId)
    │     → committedOffsets: Map<TopicPartition, OffsetAndMetadata>
    ├── adminClient.listOffsets(partitions → OffsetSpec.latest())
    │     → endOffsets: Map<TopicPartition, Long>
    └── lag = endOffset − committedOffset  (per partition)
        → returns: {groupId, status, totalLag, partitions[...]}

ConsumerLagController
└── GET /api/consumer-lag → ConsumerLagService.getConsumerLag()
```

#### Response Structure
```json
{
  "groupId": "payment-service-group",
  "status": "HEALTHY | LAGGING | NO_COMMITTED_OFFSETS | ERROR",
  "totalLag": 0,
  "partitions": [
    {"topic": "order-events", "partition": 0, "currentOffset": 42,
     "endOffset": 42, "lag": 0, "status": "CAUGHT_UP"}
  ]
}
```

---

### Service: avro-service (Phase 2 — Schema Registry + Avro)

#### Class Diagram
```
order-event.avsc (Avro schema file)
└── avro-maven-plugin (compile phase)
    └── generates: target/generated-sources/avro/
        ├── OrderEvent.java   ← SpecificRecord with builder
        └── OrderEventType.java ← Avro enum

AvroKafkaProducerConfig
└── @Bean ProducerFactory<String, OrderEvent>
    ├── VALUE_SERIALIZER = KafkaAvroSerializer.class
    └── SCHEMA_REGISTRY_URL = http://localhost:8091

AvroKafkaConsumerConfig
└── @Bean ConcurrentKafkaListenerContainerFactory
    ├── VALUE_DESERIALIZER = KafkaAvroDeserializer.class
    ├── SCHEMA_REGISTRY_URL = http://localhost:8091
    └── SPECIFIC_AVRO_READER = true  ← returns typed OrderEvent (not GenericRecord)

AvroOrderProducer
└── sendOrderEvent(customerId, productName, quantity, price)
    ├── OrderEvent.newBuilder()...build()
    └── kafkaTemplate.send(topic, customerId, event)

AvroOrderConsumer
└── @KafkaListener(topics="avro-order-events",
                   containerFactory="avroKafkaListenerContainerFactory")
    └── consume(ConsumerRecord<String, OrderEvent>)
        └── logs all fields of the deserialized OrderEvent

AvroOrderController
└── POST /api/avro/orders → AvroOrderProducer.sendOrderEvent()
```

#### Wire Format
```
[0x00][schema ID — 4 bytes][avro binary payload]
  ↑ magic byte   ↑ Schema Registry   ↑ Avro-encoded fields
```

---

### Service: Kafka Transactions (Phase 3 — order-service)

#### Class Diagram
```
OrderController
└── POST /api/v1/orders/transactional
    └── TransactionalOrderService.sendTransactionally(OrderDTO)

TransactionalOrderService
└── sendTransactionally(OrderDTO)
    └── kafkaTemplate.executeInTransaction(ops -> {
            ops.send(orderEventsTopic, customerId, placedEvent)      // topic 1
            ops.send(orderConfirmedTopic, customerId, confirmedEvent) // topic 2
            return true
        })
        // Either BOTH committed or NEITHER visible
```

#### Producer Config (application.yml)
```yaml
spring.kafka.producer:
  transaction-id-prefix: order-tx-   # enables transactional producer
  properties:
    enable.idempotence: true          # included automatically with transactions
    acks: all
```

#### Consumer Config — payment-service
```yaml
spring.kafka.consumer.properties:
  isolation.level: read_committed     # only reads committed transactions
  # Without this: consumer sees messages from aborted transactions
```

#### Transaction Lifecycle
```
beginTransaction()
  └── send to order-events         [UNCOMMITTED — not visible to read_committed consumers]
  └── send to order-confirmed-events [UNCOMMITTED]
commitTransaction()
  └── BOTH messages become visible atomically
  └── read_committed consumers now receive both messages

OR if failure:
abortTransaction()
  └── BOTH messages permanently skipped
```

---

### Kafka Connect (Phase 3 — JDBC Sink Connector)

#### Architecture
```
Kafka Topic: order-events
        │
        ▼
Kafka Connect Worker (Docker: kafka-connect:8083)
  ├── Connector: order-events-jdbc-sink
  │   ├── reads order-events topic (3 partitions)
  │   ├── applies Flatten transform (nested JSON → flat columns)
  │   └── upserts to PostgreSQL: order_events_sink table
  │
  └── Internal state (Kafka topics):
      ├── connect-configs  — connector configuration
      ├── connect-offsets  — per-partition read offsets (fault tolerant)
      └── connect-status   — connector + task health
```

#### Connector Config
```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "order-events",
  "connection.url": "jdbc:postgresql://postgres:5432/paymentdb",
  "auto.create": "true",
  "insert.mode": "upsert",
  "pk.mode": "record_value",
  "pk.fields": "orderId",
  "table.name.format": "order_events_sink",
  "transforms": "flatten",
  "transforms.flatten.type": "Flatten$Value",
  "transforms.flatten.delimiter": "_"
}
```

#### Database Table (auto-created)
```sql
-- Auto-created by Kafka Connect when first message arrives
-- Columns are derived from the flattened JSON fields
CREATE TABLE order_events_sink (
    "order_orderId"      VARCHAR PRIMARY KEY,  ← pk.fields = orderId
    "order_customerId"   VARCHAR,
    "order_productName"  VARCHAR,
    "order_price"        NUMERIC,
    "order_quantity"     INT,
    "order_status"       VARCHAR,
    "eventId"            VARCHAR,
    "eventType"          VARCHAR,
    ...
);
```

---

### SSL/SASL Security (Phase 3)

#### SASL/PLAIN Architecture
```
Kafka Broker
  └── JAAS config: kafka_server_jaas.conf
      └── KafkaServer { PlainLoginModule
              user_order-service="order-secret"
              user_payment-service="payment-secret"
              ...
          }

Client (any service)
  └── application.yml:
      security.protocol: SASL_PLAINTEXT
      sasl.mechanism: PLAIN
      sasl.jaas.config: PlainLoginModule username="order-service" password="order-secret"
```

#### Files
```
security-config/
├── kafka_server_jaas.conf     ← broker credentials (one per service)
├── docker-compose-sasl.yml    ← Kafka with SASL_PLAINTEXT listeners
├── application-sasl.yml       ← Spring Boot config snippet for all services
└── README.md                  ← SASL vs SSL, production hardening guide
```

---

### Kafka Topics — Full Reference

| Topic | Partitions | Key | Producer | Consumers | Purpose |
|---|---|---|---|---|---|
| order-events | 3 | customerId | order-service | payment, shipping, audit, kafka-connect | Main event stream |
| order-confirmed-events | 3 | customerId | order-service (TX) | — | Atomic TX partner topic |
| order-events-retry-0 | 1 | — | Spring auto | payment-service | Retry attempt 1 |
| order-events-retry-1 | 1 | — | Spring auto | payment-service | Retry attempt 2 |
| order-events-retry-2 | 1 | — | Spring auto | payment-service | Retry attempt 3 |
| order-events-dlt | 1 | — | Spring auto | dlt-handler-service | Failed messages |
| avro-order-events | 1 | customerId | avro-service | avro-service | Avro binary messages |
| order-count-per-customer | 1 | customerId | audit-service (Streams) | — | Running order count |
| revenue-per-product | 1 | productName | audit-service (Streams) | — | Running revenue total |

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
