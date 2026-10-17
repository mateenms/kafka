# payment-service

Kafka Consumer that processes payments with **Retry (3 attempts + backoff)** and **Dead Letter Topic (DLT)**.

**Port:** 8082

---

## Concepts Implemented

### 1. Retry Mechanism

**What:** When message processing fails, Kafka automatically retries before giving up.

**Why it matters:**
```
Without retry:
  Message fails → lost forever OR blocks the consumer

With retry:
  Message fails → retry after 1s → retry after 2s → retry after 4s → DLT
```

**Config via @RetryableTopic:**
```java
@RetryableTopic(
    attempts = "4",                          // 1 original + 3 retries
    backoff = @Backoff(delay = 1000, multiplier = 2.0)  // 1s → 2s → 4s
)
```

**Retry topics auto-created by Spring:**
```
order-events              ← original (attempt 1)
order-events-retry-0      ← retry 1 (after 1s)
order-events-retry-1      ← retry 2 (after 2s)
order-events-retry-2      ← retry 3 (after 4s)
order-events-dlt          ← dead letter (after all retries fail)
```

**Real-world example:** Payment gateway is temporarily down. Message retries 3 times giving the gateway time to recover. If it's still down after 3 retries, message goes to DLT for manual handling.

---

### 2. Dead Letter Topic (DLT)

**What:** A separate Kafka topic where messages go after ALL retries are exhausted.

**Why it matters:**
```
Without DLT:
  Message fails 3 times → LOST → customer never gets payment processed

With DLT:
  Message fails 3 times → moves to order-events-dlt
                        → ops team sees it
                        → can manually reprocess or alert customer
```

**DLT Handler:**
```java
@DltHandler
public void handleDlt(OrderEventDTO orderEventDTO, ...) {
    LOGGER.error("Message failed after all retries — manual intervention needed");
    // Send alert, create support ticket, notify ops team
}
```

**Real-world example:** Payment gateway is down for 10 minutes. All messages during that time go to DLT. When gateway recovers, ops team replays the DLT messages.

---

## How Retry + DLT Flow Works

```
1. order-events topic receives OrderEventDTO
2. payment-service consume() called → payment fails (exception thrown)
3. Spring retries after 1s  → still fails
4. Spring retries after 2s  → still fails
5. Spring retries after 4s  → still fails
6. Spring moves message to order-events-dlt
7. @DltHandler handleDlt() called → logs error, sends alert
```

---

## How to Trigger the Retry Demo

Send an order with `price: 1` — the consumer throws an exception intentionally:

```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "productId": "PROD-101",
    "productName": "Test Product",
    "quantity": 1,
    "price": 1
  }'
```

Watch the logs — you will see:
```
Payment failed — simulating failure for retry demo
Payment failed — simulating failure for retry demo   ← retry 1
Payment failed — simulating failure for retry demo   ← retry 2
Payment failed — simulating failure for retry demo   ← retry 3
DLT: Message failed after all retries               ← DLT handler
```

---

## Normal Flow (price != 1)

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

Logs:
```
Payment gateway approved — Amount: 999.99
Payment processed and saved — Order: <uuid>
```

---

## Project Structure

```
payment-service/
└── src/main/java/com/mateen/paymentservice/
    ├── PaymentServiceApplication.java
    ├── kafka/
    │   └── PaymentKafkaConsumer.java    # @RetryableTopic + @DltHandler
    ├── entity/
    │   └── PaymentRecord.java           # JPA entity → payment_records table
    └── repository/
        └── PaymentRecordRepository.java
```

---

## Database

Table: `payment_records` (auto-created by Hibernate)

```sql
SELECT * FROM payment_records;
```

---

## How to Run

```bash
# 1. Start Kafka + PostgreSQL
docker compose up -d
docker run -d --name postgres-payment \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=paymentdb \
  -p 5432:5432 postgres:15-alpine

# 2. Build and run
mvn clean install
java -jar target/payment-service-1.0.0.jar
```
