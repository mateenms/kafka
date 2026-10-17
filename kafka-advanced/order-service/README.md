# order-service

REST API that receives orders and publishes them to Kafka using an **Idempotent Producer** with **Partitioning by customerId**.

**Port:** 8081

---

## Concepts Implemented

### 1. Idempotent Producer

**What:** Guarantees each message is written to Kafka **exactly once**, even if the producer retries.

**Why it matters:**
```
Without idempotence:
  Producer sends message ──▶ Kafka saves ──▶ network drops ACK
  Producer retries       ──▶ Kafka saves AGAIN ──▶ DUPLICATE message!

With idempotence:
  Producer sends message ──▶ Kafka saves (sequence #1) ──▶ ACK lost
  Producer retries       ──▶ Kafka sees sequence #1 already exists ──▶ DISCARDS duplicate
```

**Config in application.yml:**
```yaml
spring.kafka.producer.properties:
  enable.idempotence: true   # Enable idempotent producer
  acks: all                  # All replicas must confirm (required for idempotence)
  retries: 3                 # Retry up to 3 times on failure
  max.in.flight.requests.per.connection: 5  # Max 5 unconfirmed requests (safe with idempotence)
```

**Real-world example:** Customer clicks "Place Order" twice due to slow network. Without idempotence, 2 orders created. With idempotence, exactly 1 order created.

---

### 2. Partitioning by Key (customerId)

**What:** Uses `customerId` as the Kafka message key so all orders from the same customer always go to the **same partition**.

**Why it matters:**
```
Without key (round-robin):
  CUST-001 Order-1 → Partition 0
  CUST-001 Order-2 → Partition 2   ← processed before Order-1!
  CUST-001 Order-3 → Partition 1   ← wrong order!

With customerId as key:
  CUST-001 Order-1 → Partition 0   ← always same partition
  CUST-001 Order-2 → Partition 0   ← guaranteed order
  CUST-001 Order-3 → Partition 0   ← always in sequence
  CUST-002 Order-1 → Partition 1   ← different customer, different partition
```

**Code:**
```java
// Key = customerId ensures same customer always hits same partition
kafkaTemplate.send(topic, orderEventDTO.getOrder().getCustomerId(), orderEventDTO);
```

**Real-world example:** Customer places order, then immediately cancels. Without key partitioning, cancel could be processed before place — causing errors. With key partitioning, they're always in the right order.

---

## API

### Place Order
```
POST http://localhost:8081/api/v1/orders
Content-Type: application/json

{
    "customerId": "CUST-001",
    "productId": "PROD-101",
    "productName": "iPhone 15",
    "quantity": 2,
    "price": 999.99
}
```

**Response:**
```
Order placed successfully — ID: <uuid>
```

---

## Project Structure

```
order-service/
└── src/main/java/com/mateen/orderservice/
    ├── OrderServiceApplication.java
    ├── controller/
    │   └── OrderController.java       # POST /api/v1/orders
    ├── kafka/
    │   └── OrderKafkaProducer.java    # Idempotent producer + partition by customerId
    └── config/
        └── KafkaTopicConfig.java      # Creates order-events topic (3 partitions)
```

---

## How to Run

```bash
# 1. Start Kafka
docker compose up -d

# 2. Build base-domain first
cd ../base-domain && mvn clean install

# 3. Build and run order-service
cd ../order-service && mvn clean install
java -jar target/order-service-1.0.0.jar
```

---

## What to Watch in Logs

When you hit the POST endpoint:
```
Sending order event to topic: order-events
Partition key (customerId): CUST-001
Order event sent successfully
Topic    : order-events
Partition: 0          ← CUST-001 always goes to same partition
Offset   : 0
```

Send another order with same customerId — notice it always goes to the **same partition**.
Send with a different customerId — it goes to a **different partition**.

---

## Kafka Topic

| Topic | Partitions | Key | Purpose |
|---|---|---|---|
| `order-events` | 3 | customerId | Guarantees order per customer |
