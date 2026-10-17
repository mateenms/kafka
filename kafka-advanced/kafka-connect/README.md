# Kafka Connect — JDBC Sink Connector

## What is Kafka Connect?

Kafka Connect is a **framework** for moving data between Kafka and external systems (databases, file systems, S3, Elasticsearch, etc.) **without writing any application code**.

You configure a connector (a JSON file) and Kafka Connect does the rest:

```
Kafka Topic (order-events)
        │
        ▼
  Kafka Connect
  (JDBC Sink Connector)
        │
        ▼
  PostgreSQL Table (order_events_sink)
```

---

## Source vs Sink Connectors

| Type | Direction | Example use case |
|------|-----------|------------------|
| **Source Connector** | External system → Kafka | CDC: stream DB changes into Kafka |
| **Sink Connector** | Kafka → External system | Write Kafka messages to PostgreSQL, S3, Elasticsearch |

This service implements a **JDBC Sink Connector** — it reads the `order-events` topic and writes each message to a PostgreSQL table.

---

## How Kafka Connect Works

```
┌─────────────────────────────────────────────────────────┐
│  Kafka Connect Worker (JVM process)                      │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Connector: order-events-jdbc-sink               │   │
│  │                                                  │   │
│  │  Task 1:                                         │   │
│  │    reads order-events partition 0                │   │
│  │    writes to order_events_sink table             │   │
│  │                                                  │   │
│  │  Task 2:                                         │   │
│  │    reads order-events partition 1                │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  Internal topics (auto-created):                        │
│    connect-configs  ← connector configs stored here     │
│    connect-offsets  ← how far each task has read        │
│    connect-status   ← connector/task health             │
└─────────────────────────────────────────────────────────┘
```

Kafka Connect stores its own state in Kafka topics — this means it's fault-tolerant and restartable. If it restarts, it resumes from exactly where it left off (using `connect-offsets`).

---

## Connector Config Explained

```json
{
  "name": "order-events-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",

    "topics": "order-events",          ← which topic to read from

    "connection.url": "jdbc:postgresql://postgres:5432/paymentdb",
    "connection.user": "postgres",
    "connection.password": "postgres",

    "auto.create": "true",             ← create the table if it doesn't exist
    "auto.evolve": "true",             ← add new columns when schema changes

    "insert.mode": "upsert",           ← INSERT or UPDATE if row with same pk exists
    "pk.mode": "record_value",         ← use a field from the message value as primary key
    "pk.fields": "orderId",            ← orderId is the primary key

    "table.name.format": "order_events_sink",  ← destination table name

    "transforms": "flatten",
    "transforms.flatten.type": "Flatten$Value",  ← nested JSON → flat columns
    "transforms.flatten.delimiter": "_"          ← nested.field → nested_field
  }
}
```

### `insert.mode: upsert`

If the same `orderId` appears twice (retry scenario), the row is updated instead of duplicated. This ensures idempotent writes to PostgreSQL.

### `auto.create: true`

Kafka Connect reads the schema of the first message and creates the table automatically. With `auto.evolve: true`, new fields in messages add columns to the table.

### `transforms: flatten`

Our `OrderEventDTO` is nested JSON:
```json
{
  "order": {
    "orderId": "123",
    "customerId": "CUST001"
  }
}
```

After flatten with `_` delimiter:
```
order_orderId    → "123"
order_customerId → "CUST001"
```

Each becomes a separate column in PostgreSQL.

---

## How to Start

### 1. Start with Docker Compose

```bash
# Build the connect image (installs JDBC plugin)
docker-compose up -d kafka-connect

# Check it's running (takes ~30-40 seconds)
curl http://localhost:8083/connectors
# Returns: []
```

### 2. Register the Connector

```bash
# Run the registration script
bash kafka-connect/register-connector.sh

# Or manually:
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @kafka-connect/connectors/order-events-sink.json
```

### 3. Verify Connector Status

```bash
curl http://localhost:8083/connectors/order-events-jdbc-sink/status
```

Expected response:
```json
{
  "name": "order-events-jdbc-sink",
  "connector": {"state": "RUNNING"},
  "tasks": [{"id": 0, "state": "RUNNING"}]
}
```

### 4. Send an Order

```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "CUST001", "productName": "Laptop", "price": 1299.99, "quantity": 1}'
```

### 5. Verify in PostgreSQL

```bash
docker exec -it postgres-payment psql -U postgres -d paymentdb \
  -c "SELECT * FROM order_events_sink;"
```

---

## Connector Management REST API

Kafka Connect exposes a full REST API at `http://localhost:8083`:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/connectors` | List all connectors |
| POST | `/connectors` | Register a new connector |
| GET | `/connectors/{name}/status` | Check connector health |
| PUT | `/connectors/{name}/config` | Update connector config |
| POST | `/connectors/{name}/restart` | Restart a connector |
| DELETE | `/connectors/{name}` | Remove a connector |

---

## Key Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Extends Confluent Kafka Connect image, installs JDBC plugin + PostgreSQL driver |
| `connectors/order-events-sink.json` | Connector config — defines topic, DB, table, transforms |
| `register-connector.sh` | Script to POST the connector config to Kafka Connect |

---

## Phase in Project

This feature is part of **Phase 3 — Production Kafka**:

- Phase 1 ✅ — Idempotent Producer, Partitioning, Retry + DLT, Global Error Handler
- Phase 2 ✅ — Kafka Streams, Consumer Lag Monitoring, Schema Registry + Avro
- **Phase 3 (this)** — Kafka Transactions, **Kafka Connect**, SSL/SASL
