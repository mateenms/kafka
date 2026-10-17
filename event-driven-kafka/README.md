# Event-Driven Microservices with Apache Kafka

A production-ready event-driven architecture built with Spring Boot 3, Apache Kafka, ELK Stack, and Zipkin distributed tracing.

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────────────┐
                        │                   EVENT FLOW                        │
                        └─────────────────────────────────────────────────────┘

  [Postman / Client]
        │
        │  POST /api/v1/order/orders
        ▼
  ┌─────────────────┐
  │  order-service  │  ──── publishes ────▶  [ Kafka: order_topic ]
  │   (Port 8081)   │                               │
  └─────────────────┘                               │
                                                    ├──────────────────────────────┐
                                                    ▼                              ▼
                                        ┌───────────────────┐         ┌────────────────────────┐
                                        │ inventory-service │         │  notification-service  │
                                        │   (Port 8083)     │         │     (Port 8082)        │
                                        │                   │         │                        │
                                        │  Saves to         │         │  Logs & sends          │
                                        │  PostgreSQL DB    │         │  email notification    │
                                        └───────────────────┘         └────────────────────────┘
```

---

## Services

| Service               | Port | Role                                              |
|-----------------------|------|---------------------------------------------------|
| order-service         | 8081 | REST API — receives orders, publishes Kafka events |
| inventory-service     | 8083 | Consumes events — reserves stock, saves to DB      |
| notification-service  | 8082 | Consumes events — sends email notifications        |
| base-domain           | —    | Shared DTOs and enums (no server)                  |

---

## Infrastructure

| Tool            | Port  | Purpose                              |
|-----------------|-------|--------------------------------------|
| Apache Kafka    | 9092  | Message broker                       |
| Zookeeper       | 2181  | Kafka cluster coordinator            |
| Kafka UI        | 8080  | Visual Kafka topic browser           |
| Elasticsearch   | 9200  | Stores structured logs               |
| Logstash        | 5000  | Receives and ships logs to ES        |
| Kibana          | 5601  | Log visualisation dashboard          |
| Zipkin          | 9411  | Distributed tracing UI               |
| PostgreSQL      | 5432  | Persistent storage for inventory     |

---

## Tech Stack

- **Java 17** + **Spring Boot 3.3.4**
- **Apache Kafka** (Confluent cp-kafka:7.6.0)
- **Spring Kafka** — `KafkaTemplate` (producer), `@KafkaListener` (consumer)
- **ELK Stack** — Elasticsearch 8.15.3 + Logstash 8.15.3 + Kibana 8.15.3
- **Zipkin 3.4** — distributed tracing via Micrometer Brave
- **Logstash Logback Encoder 7.4** — JSON structured logs over TCP
- **PostgreSQL 15** — inventory persistence
- **Spring Data JPA + Hibernate**
- **Lombok** + **MapStruct 1.6.2**
- **Docker Compose** — full infrastructure stack

---

## Project Structure

```
event-driven-kafka/
│
├── base-domain/                  # Shared DTOs and enums
│   └── src/main/java/com/basedomain/
│       ├── dto/
│       │   ├── OrderDTO.java
│       │   └── OrderEventDTO.java
│       └── enums/
│           ├── OrderStatus.java
│           └── OrderEventType.java
│
├── order-service/                # Producer — Port 8081
│   └── src/main/java/com/mateen/orderservice/
│       ├── controller/OrderController.java
│       ├── config/KafkaTopicConfig.java
│       └── kafka/OrderKafkaProducer.java
│
├── inventory-service/            # Consumer — Port 8083
│   └── src/main/java/com/mateen/inventoryservice/
│       ├── entity/OrderInventory.java
│       ├── repository/OrderInventoryRepository.java
│       └── kafka/OrderConsumer.java
│
├── notification-service/         # Consumer — Port 8082
│   └── src/main/java/com/mateen/notificationservice/
│       └── kafka/KafkaEmailConsumer.java
│
├── logstash/pipeline/
│   └── logstash.conf             # Log pipeline config
│
└── docker-compose.yml            # Full infrastructure stack
```

---

## How to Run

### Prerequisites

- Java 17
- Maven 3.8+
- Docker Desktop running

---

### Step 1 — Build base-domain first

```bash
cd base-domain
mvn clean install
```

---

### Step 2 — Start infrastructure (Kafka, ELK, Zipkin, PostgreSQL)

```bash
# Start Kafka + ELK + Zipkin
docker compose up -d

# Start PostgreSQL
docker run -d \
  --name postgres-db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=inventorydb \
  -p 5432:5432 \
  postgres:15-alpine
```

Wait ~30 seconds for all containers to be healthy.

---

### Step 3 — Build all services

```bash
cd order-service        && mvn clean install -q && cd ..
cd inventory-service    && mvn clean install -q && cd ..
cd notification-service && mvn clean install -q && cd ..
```

---

### Step 4 — Start all services

Open 3 separate terminals:

```bash
# Terminal 1
java -jar order-service/target/order-service-1.0.0.jar

# Terminal 2
java -jar inventory-service/target/inventory-service-1.0.0.jar

# Terminal 3
java -jar notification-service/target/notification-service-1.0.0.jar
```

---

### Step 5 — Test the flow

**Place an order via Postman or curl:**

```bash
curl -X POST http://localhost:8081/api/v1/order/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "productId": "PROD-101",
    "productName": "iPhone 15",
    "quantity": 2,
    "price": 999.99
  }'
```

**Expected response:**
```
Order placed Successfully
```

---

### Step 6 — Verify

| What to check              | Where                                          |
|----------------------------|------------------------------------------------|
| Kafka message              | http://localhost:8080 → Topics → order_topic   |
| Inventory saved to DB      | `docker exec postgres-db psql -U postgres -d inventorydb -c "SELECT * FROM order_inventory;"` |
| Logs in Kibana             | http://localhost:5601 → Discover → spring-logs-* |
| Distributed traces         | http://localhost:9411 → Run Query              |
| Service health             | http://localhost:8081/actuator/health          |

---

## Kafka Topic

| Topic        | Partitions | Replicas | Produced by   | Consumed by                              |
|--------------|------------|----------|---------------|------------------------------------------|
| order_topic  | 3          | 1        | order-service | inventory-service, notification-service  |

---

## Event Flow (Step by Step)

1. Client sends `POST /api/v1/order/orders` with order details
2. `order-service` assigns `orderId`, sets status `PENDING`, wraps in `OrderEventDTO`
3. `OrderKafkaProducer` publishes the event to `order_topic`
4. **inventory-service** consumes the event → logs details → saves record to PostgreSQL
5. **notification-service** consumes the event → logs details → triggers email notification
6. All logs flow via Logstash TCP → Elasticsearch → visible in Kibana
7. All HTTP traces are exported to Zipkin for end-to-end tracing

---

## Health Checks

```
http://localhost:8081/actuator/health   # order-service
http://localhost:8082/actuator/health   # notification-service
http://localhost:8083/actuator/health   # inventory-service
```
