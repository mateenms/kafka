# dlt-handler-service

Dedicated service that **only** consumes from the Dead Letter Topic (`order-events-dlt`).
It receives messages that failed all retries in payment-service and handles them — logging, alerting, and triggering manual intervention.

**Port:** 8085

---

## Concept Implemented — Dead Letter Topic Consumer

### What

Dead Letter Topic (DLT) is a special Kafka topic where messages go after ALL retries are exhausted.
This service is the dedicated consumer of that topic.

### Why it matters

```
Without DLT handler:
  Message fails 3 retries → moved to DLT topic → sits there unread
  No one knows about the failure → customer never hears back

With DLT handler:
  Message fails 3 retries → moved to order-events-dlt
  dlt-handler-service picks it up → logs it → alerts ops team
  Ops team investigates and manually reprocesses or refunds
```

### Full Flow

```
order-events topic
      │
      ▼
payment-service.consume() → FAILS
      │
      ├── retry after 1s  → FAILS
      ├── retry after 2s  → FAILS
      ├── retry after 4s  → FAILS
      │
      ▼
order-events-dlt topic
      │
      ▼
dlt-handler-service.consumeFromDlt()
      ├── logs full message details at ERROR level
      ├── [real-world] send alert email to ops team
      ├── [real-world] create Jira / PagerDuty ticket
      ├── [real-world] notify customer of payment failure
      └── [real-world] store in DB for audit trail
```

---

## How to Trigger DLT (Demo)

Send an order with `price: 1` — payment-service simulates failure for this price:

```bash
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-FAIL",
    "productId": "PROD-001",
    "productName": "Fail Test Product",
    "quantity": 1,
    "price": 1
  }'
```

Watch dlt-handler-service logs:
```
========================================
  DEAD LETTER TOPIC — FAILED MESSAGE
========================================
DLT Topic : order-events-dlt
Partition : 0 | Offset: 0
Order ID  : <uuid>
Customer  : CUST-FAIL
Product   : Fail Test Product
Amount    : 1
------ Required Actions ------
1. Investigate root cause of payment failure
2. Notify customer: CUST-FAIL
3. Manually reprocess or refund order: <uuid>
========================================
```

---

## Project Structure

```
dlt-handler-service/
└── src/main/java/com/mateen/dlthandlerservice/
    ├── DltHandlerServiceApplication.java
    └── kafka/
        └── DltKafkaConsumer.java   # Listens ONLY to order-events-dlt
```

---

## Key Difference from Other Consumers

| | payment-service | dlt-handler-service |
|---|---|---|
| Topic | order-events | order-events-dlt |
| Purpose | Process payments | Handle failures |
| On failure | Retry + DLT | Log + Alert |
| Group ID | payment-service-group | dlt-handler-group |

---

## How to Run

```bash
# Start infrastructure first
docker compose up -d

# Build and run
mvn clean install
java -jar target/dlt-handler-service-1.0.0.jar
```
