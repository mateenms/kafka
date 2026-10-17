# shipping-service

Kafka Consumer that processes shipments using a **Global Error Handler** with retry and non-retryable exception support.

**Port:** 8083

---

## Concept Implemented — Global Error Handler

### What

A single `DefaultErrorHandler` bean that applies to **ALL** Kafka listeners in this service automatically.
No need to configure error handling on each individual `@KafkaListener`.

### Why it matters

```
Without error handler:
  consume() throws exception → Kafka retries indefinitely → consumer gets stuck

With Global Error Handler:
  consume() throws exception → retries 3x with 1s interval
  After 3 retries → error handler logs it → consumer moves to NEXT message
```

### How it works

```java
@Bean
public DefaultErrorHandler errorHandler() {
    FixedBackOff fixedBackOff = new FixedBackOff(1000L, 3L); // 1s interval, 3 attempts

    return new DefaultErrorHandler((record, exception) -> {
        // Called when ALL retries exhausted
        // Log error, send alert, create ticket
    }, fixedBackOff);
}
```

### Non-Retryable Exceptions

Some exceptions will ALWAYS fail — no point retrying them:
```java
errorHandler.addNotRetryableExceptions(
    IllegalArgumentException.class,   // bad data — retry won't fix it
    NullPointerException.class        // bad data — retry won't fix it
);
```

### Global Error Handler vs @RetryableTopic (payment-service)

| | Global Error Handler | @RetryableTopic |
|---|---|---|
| Config location | One `@Bean` | On each `@KafkaListener` |
| Retry storage | In-memory | Separate Kafka retry topics |
| Survives restart | ❌ No | ✅ Yes |
| Dead Letter Topic | ❌ Manual | ✅ Auto |
| Best for | Lightweight services | Critical payment/order flows |

**Rule of thumb:**
- Use `@RetryableTopic` when the message MUST NOT be lost (payments, orders)
- Use `GlobalErrorHandler` when occasional message loss is acceptable (shipment scheduling)

---

## Project Structure

```
shipping-service/
└── src/main/java/com/mateen/shippingservice/
    ├── ShippingServiceApplication.java
    ├── config/
    │   └── KafkaErrorHandlerConfig.java   # Global error handler bean
    └── kafka/
        └── ShippingKafkaConsumer.java     # @KafkaListener — error handler auto-applied
```

---

## How to Run

```bash
# Start infrastructure first
docker compose up -d

# Build and run
mvn clean install
java -jar target/shipping-service-1.0.0.jar
```

---

## What to Watch in Logs

Normal flow:
```
Shipment scheduled for order: <uuid>
Shipment label created for order: <uuid>
Shipment Processing Complete
```

If an exception is thrown (simulate by adding `throw new RuntimeException()`):
```
[WARN] Retry 1/3 for message...
[WARN] Retry 2/3 for message...
[WARN] Retry 3/3 for message...
[ERROR] Global Error Handler: All retries exhausted
[ERROR] Topic: order-events | Partition: 0 | Offset: 5
[ERROR] Message skipped — investigate manually
```
