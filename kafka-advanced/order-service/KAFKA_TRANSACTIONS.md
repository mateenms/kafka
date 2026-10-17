# Kafka Transactions

## What is a Kafka Transaction?

A Kafka transaction lets a producer send messages to **multiple topics/partitions atomically** — either ALL messages are committed and visible to consumers, or NONE are.

Without transactions:
```
Producer sends to order-events      ✓ success
Producer sends to order-confirmed   ✗ failure (network error)
→ Consumers see order-events message but NOT the confirmation
→ System is in an inconsistent state
```

With transactions:
```
Producer starts transaction
Producer sends to order-events      (not yet visible)
Producer sends to order-confirmed   (not yet visible)
Producer commits transaction
→ BOTH messages become visible at the same time
→ Or if commit fails, NEITHER is visible
```

---

## How It Works Internally

```
1. beginTransaction()
   → Kafka assigns a unique transaction ID to this producer

2. send(topic1, key, value)
   → Message is written to the topic but marked as UNCOMMITTED
   → read_committed consumers skip it

3. send(topic2, key, value)
   → Same — written but not visible

4. commitTransaction()
   → Kafka writes a commit marker to both partitions
   → Messages become visible to read_committed consumers simultaneously

5. If anything fails → abortTransaction()
   → Kafka writes an abort marker
   → The messages are permanently skipped by consumers
```

---

## Configuration

### Producer (order-service application.yml)

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: order-tx-
      # transaction-id-prefix enables transactional mode
      # Spring appends a number per instance: order-tx-0, order-tx-1
      # Each unique ID gets its own producer epoch in Kafka
```

When `transaction-id-prefix` is set, Spring auto-configures a `KafkaTransactionManager` and the `KafkaTemplate` becomes transactional.

### Consumer (payment-service application.yml)

```yaml
spring:
  kafka:
    consumer:
      properties:
        isolation.level: read_committed
        # read_uncommitted (default): sees ALL messages including those from in-flight or aborted transactions
        # read_committed: ONLY sees messages from committed transactions
```

**If you don't set `read_committed`, your consumer will see messages from aborted transactions — defeating the purpose of transactions.**

---

## Code Pattern

### Producer — executeInTransaction

```java
kafkaTemplate.executeInTransaction(ops -> {
    ops.send(orderEventsTopic, customerId, placedEvent);      // send 1
    ops.send(orderConfirmedTopic, customerId, confirmedEvent); // send 2
    return true;
    // If either send throws → both are rolled back automatically
});
```

### Producer — @Transactional (alternative)

```java
@Transactional
public void sendBoth(OrderEventDTO event1, OrderEventDTO event2) {
    kafkaTemplate.send(topic1, key, event1);
    kafkaTemplate.send(topic2, key, event2);
}
```

Both patterns achieve the same result. `executeInTransaction` is more explicit.

---

## API Endpoint

```
POST /api/v1/orders/transactional
```

Sends `ORDER_PLACED` to `order-events` AND `ORDER_CONFIRMED` to `order-confirmed-events` in a single atomic transaction.

```bash
curl -X POST http://localhost:8081/api/v1/orders/transactional \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST001",
    "productName": "Laptop",
    "price": 1299.99,
    "quantity": 1
  }'
```

---

## Idempotence vs Transactions

| | Idempotent Producer | Transactional Producer |
|-|--------------------|-----------------------|
| Prevents | Duplicate messages on retry | Partial writes across topics |
| Scope | Single producer → single partition | Multiple topics/partitions |
| Config | `enable.idempotence: true` | `transaction-id-prefix: ...` |
| Consumer change | None needed | Set `isolation.level: read_committed` |
| Overhead | Very low | Small (transaction markers) |

Transactions include idempotence automatically — you don't need both.

---

## When to Use Transactions

Use Kafka transactions when:
- You write to 2+ topics and both must succeed together
- You implement the **consume-transform-produce** pattern (read from one topic, process, write to another — all atomic)
- You need exactly-once semantics end-to-end

Do NOT use when:
- You write to only one topic (idempotent producer is enough)
- You only need to prevent retries (that's what idempotence is for)

---

## Phase in Project

This feature is part of **Phase 3 — Production Kafka**:

- Phase 1 ✅ — Idempotent Producer, Partitioning, Retry + DLT, Global Error Handler
- Phase 2 ✅ — Kafka Streams, Consumer Lag Monitoring, Schema Registry + Avro
- **Phase 3 (this)** — **Kafka Transactions**, Kafka Connect, SSL/SASL
