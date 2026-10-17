# Avro Service — Schema Registry + Avro Serialization

## What This Service Does

This service demonstrates how to use **Apache Avro** as the message format and **Confluent Schema Registry** as the central schema store for Kafka messages.

It is a self-contained demo: it both **produces** and **consumes** Avro-serialized messages on the `avro-order-events` topic.

---

## What is JSON vs Avro?

| | JSON | Avro |
|-|------|------|
| Format | Human-readable text | Compact binary |
| Size | Large (field names repeated every message) | Small (schema shared separately) |
| Schema | Optional | Mandatory (enforced) |
| Type safety | Loose (any field can be missing) | Strict (fields defined in schema) |
| Schema evolution | Manual, error-prone | Built-in, backwards compatible |
| Speed | Slower to parse | Fast (binary encoding) |

With JSON, every Kafka message looks like:
```json
{"eventId":"abc","customerId":"CUST001","productName":"Laptop","price":"1299.99",...}
```

With Avro, every Kafka message is binary:
```
[0x00][schema-id (4 bytes)][avro binary payload]
```

The field names are NOT in the message — they're in the schema stored in Schema Registry.

---

## What is Schema Registry?

**Schema Registry** is a server (Confluent's open-source component) that stores Avro schemas and gives each schema a unique integer ID.

```
┌───────────────────┐      register schema (first time)     ┌──────────────────────┐
│  Avro Producer    │ ─────────────────────────────────────▶ │  Schema Registry     │
│                   │ ◀─────────────────────────────────────  │  (stores schemas)    │
│                   │      returns schema ID (e.g. 1)         │  http://localhost:8091│
│                   │                                         └──────────────────────┘
│  sends to Kafka:  │
│  [0x00][1][binary]│ ──────────────────────────────────────▶ Kafka Topic
└───────────────────┘

┌───────────────────┐      fetch schema by ID=1             ┌──────────────────────┐
│  Avro Consumer    │ ─────────────────────────────────────▶ │  Schema Registry     │
│                   │ ◀─────────────────────────────────────  │                      │
│                   │      returns schema definition          └──────────────────────┘
│  reads from Kafka │
│  deserializes     │ ◀────────────────────────────────────── Kafka Topic
│  → OrderEvent obj │
└───────────────────┘
```

**Why is this better than putting the schema in every message?**
- Message payload is tiny (no field names repeated)
- Schema is versioned and tracked centrally
- Producer and consumer can use different schema versions (backwards compatibility)

---

## What is Schema Evolution?

This is the biggest benefit of Schema Registry. You can **add new fields** to your Avro schema and old consumers won't break.

**Example:**

Version 1 of schema:
```json
{"name": "eventId", "type": "string"},
{"name": "customerId", "type": "string"}
```

Version 2 of schema — added a new optional field:
```json
{"name": "eventId", "type": "string"},
{"name": "customerId", "type": "string"},
{"name": "region", "type": ["null", "string"], "default": null}   ← NEW
```

Old consumers reading Version 1 messages still work — `region` is just absent.
New consumers reading Version 1 messages still work — `region` defaults to `null`.

**This is impossible to do safely with JSON without careful coordination.**

---

## The Avro Schema (order-event.avsc)

```json
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.mateen.avro",
  "fields": [
    {"name": "eventId",     "type": "string"},
    {"name": "eventType",   "type": {"type": "enum", "name": "OrderEventType",
                                     "symbols": ["ORDER_PLACED", "ORDER_CONFIRMED", ...]}},
    {"name": "orderId",     "type": "string"},
    {"name": "customerId",  "type": "string"},
    {"name": "productName", "type": "string"},
    {"name": "quantity",    "type": "int"},
    {"name": "price",       "type": "string"},
    {"name": "status",      "type": "string"},
    {"name": "message",     "type": ["null", "string"], "default": null},  ← nullable
    {"name": "createdAt",   "type": "string"}
  ]
}
```

The `message` field is **nullable** (`["null", "string"]` with `"default": null`). This is the Avro pattern for optional fields — it supports schema evolution.

---

## How Java Classes Are Generated (avro-maven-plugin)

You do NOT write `OrderEvent.java` by hand. The **avro-maven-plugin** generates it from the `.avsc` file at build time:

```
src/main/avro/order-event.avsc
        │
        ▼ (mvn compile)
target/generated-sources/avro/com/mateen/avro/
        ├── OrderEvent.java       ← generated SpecificRecord class
        └── OrderEventType.java   ← generated enum
```

The generated `OrderEvent` class has:
- A builder: `OrderEvent.newBuilder().setEventId(...).build()`
- Getters: `event.getCustomerId()`, `event.getProductName()`, etc.
- Implements `SpecificRecord` (Avro interface)

---

## SpecificRecord vs GenericRecord

| | SpecificRecord | GenericRecord |
|-|----------------|---------------|
| How | Generated Java class from .avsc | Raw map-like object |
| Access | `event.getCustomerId()` | `event.get("customerId")` |
| Type safety | Compile-time | Runtime only |
| Use case | When you own the schema | When schema is unknown at compile time |

This service uses **SpecificRecord** (production best practice).

---

## Key Classes

| File | Purpose |
|------|---------|
| `src/main/avro/order-event.avsc` | Avro schema definition (source of truth) |
| `AvroKafkaProducerConfig.java` | Sets up `KafkaTemplate<String, OrderEvent>` with `KafkaAvroSerializer` |
| `AvroKafkaConsumerConfig.java` | Sets up consumer factory with `KafkaAvroDeserializer` + `SPECIFIC_AVRO_READER=true` |
| `AvroOrderProducer.java` | Builds `OrderEvent` using the builder pattern and sends to Kafka |
| `AvroOrderConsumer.java` | Reads `OrderEvent` from Kafka — fully deserialized, type-safe |
| `AvroOrderController.java` | REST endpoint to trigger an Avro message |

---

## Application Config

| Property | Value |
|----------|-------|
| Port | 8086 |
| Topic | `avro-order-events` |
| Schema Registry | `http://localhost:8091` |
| Consumer Group | `avro-service-group` |

---

## How to Run

### Prerequisites

```bash
# Start Kafka + Schema Registry
docker-compose up -d zookeeper kafka schema-registry kafka-ui
```

Verify Schema Registry is up:
```bash
curl http://localhost:8091/subjects
# Returns: [] (empty list — no schemas registered yet)
```

### Start the Service

```bash
mvn spring-boot:run -f avro-service/pom.xml
```

### Send an Avro Order

```bash
curl -X POST http://localhost:8086/api/avro/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST001",
    "productName": "Laptop",
    "quantity": 1,
    "price": "1299.99"
  }'
```

### Verify Schema Registration

```bash
# List all registered subjects (one subject per topic-value/topic-key)
curl http://localhost:8091/subjects

# Returns: ["avro-order-events-value"]
# Subject name = <topic-name>-value (Confluent naming convention)

# Get the registered schema
curl http://localhost:8091/subjects/avro-order-events-value/versions/latest
```

### Check in Kafka UI

Open `http://localhost:8080` → Schema Registry tab → you'll see `avro-order-events-value` with the full Avro schema.

---

## Sample Log Output

**Producer:**
```
Sending Avro OrderEvent — customerId: CUST001 | product: Laptop | price: 1299.99
Avro message sent — topic: avro-order-events | partition: 0 | offset: 0
```

**Consumer:**
```
═══════════════════════════════════════════
Avro message received from Schema Registry
Partition key (customerId) : CUST001
Event ID                   : 550e8400-e29b-41d4-a716-446655440000
Event Type                 : ORDER_PLACED
Order ID                   : 7f3d2b10-...
Product                    : Laptop
Quantity                   : 1
Price                      : 1299.99
Status                     : PENDING
Message                    : null
Created At                 : 2026-10-17T12:00:00
Topic                      : avro-order-events
Partition                  : 0
Offset                     : 0
═══════════════════════════════════════════
```

---

## How This Differs from JSON (order-service)

| Aspect | order-service (JSON) | avro-service (Avro) |
|--------|---------------------|---------------------|
| Serializer | `JsonSerializer` | `KafkaAvroSerializer` |
| Deserializer | `JsonDeserializer` | `KafkaAvroDeserializer` |
| Schema enforcement | None | Schema Registry |
| Message format | Readable JSON string | Binary bytes |
| Schema storage | In Java class only | Schema Registry server |
| Schema versioning | Manual | Automatic (per subject) |
| Extra config | None | `schema.registry.url` required |

---

## Phase in Project

This service is part of **Phase 2 — Intermediate Kafka**:

- Phase 1 ✅ — Idempotent Producer, Partitioning, Retry + DLT, Global Error Handler
- Phase 2 ✅ — Kafka Streams, Consumer Lag Monitoring, **Schema Registry + Avro**
- Phase 3 — Kafka Transactions, Kafka Connect, SSL/SASL
