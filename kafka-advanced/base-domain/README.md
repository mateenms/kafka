# base-domain

Shared library containing all DTOs and enums used across kafka-advanced microservices.

## Package: `com.basedomain`

### DTOs
| Class | Used By | Purpose |
|---|---|---|
| `OrderDTO` | all services | Core order data |
| `OrderEventDTO` | all services | Kafka event wrapper for order |
| `PaymentEventDTO` | payment-service, dlt-handler | Payment event data |

### Enums
| Enum | Values |
|---|---|
| `OrderStatus` | PENDING, CONFIRMED, PROCESSING, SHIPPED, DELIVERED, CANCELLED, FAILED |
| `OrderEventType` | ORDER_PLACED, ORDER_CONFIRMED, ORDER_CANCELLED, ORDER_FAILED |
| `PaymentStatus` | PENDING, SUCCESS, FAILED, REFUNDED |

## Build

Must be built first before any service:
```bash
cd base-domain
mvn clean install
```
