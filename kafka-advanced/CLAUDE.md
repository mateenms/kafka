# kafka-advanced — Full Session Context for Claude

> Read this file fully before starting any work. This captures the complete history,
> what was built, what was learned, and exactly what to build next.

---

## Who is the User

- **Name:** Mohammad Mateen
- **Email:** javamateen@gmail.com
- **Java version:** 17 (local machine — not 21)
- **OS:** macOS
- **IDE:** VS Code with Claude Code extension
- **Experience level:** Comfortable with Spring Boot 3, Kafka basics, Docker, PostgreSQL
- **Learning goal:** Master Apache Kafka from basics to production-grade microservices

---

## Full Journey So Far

### Project 1 — kafka001 (Basics)
**Location:** `/Users/mohammadmateen/kafka/kafka001/`

Built a basic Spring Boot app to understand Kafka fundamentals:
- String producer/consumer
- JSON producer/consumer (User object)
- Two topics: `myTopic` (String), `secondTopic` (JSON)
- REST API to trigger messages
- **Learned:** How Kafka works, topics, partitions, consumer groups, serialization

---

### Project 2 — kafka02-wikimedia (Real-Time Pipeline)
**Location:** `/Users/mohammadmateen/kafka/kafka02-wikimedia/`

Built a real-time data pipeline:
- **Producer:** Reads live Wikipedia edit events via SSE (Server-Sent Events)
- **Consumer:** Consumes events from Kafka, saves to PostgreSQL
- **Learned:** Real-world streaming, SSE integration, DB persistence from Kafka

---

### Project 3 — event-driven-kafka (Microservices) ← MAIN PROJECT
**Location:** `/Users/mohammadmateen/kafka/event-driven-kafka/`
**GitHub:** https://github.com/mateenms/kafka

#### Services Built:
| Service | Port | Role |
|---|---|---|
| `order-service` | 8081 | REST API — receives orders, publishes `OrderEventDTO` to Kafka |
| `inventory-service` | 8083 | Consumes events, saves `OrderInventory` to PostgreSQL |
| `notification-service` | 8082 | Consumes events, logs email notifications |
| `base-domain` | — | Shared DTOs (OrderDTO, OrderEventDTO) and enums |

#### Key Classes:
- `OrderController` → `POST /api/v1/order/orders` — sets orderId, status PENDING, createdAt
- `OrderKafkaProducer` → uses `KafkaTemplate<String, OrderEventDTO>` + `MessageBuilder`
- `KafkaTopicConfig` → creates `NewTopic` bean with `TopicBuilder` (3 partitions, 1 replica)
- `OrderConsumer` → `@KafkaListener` with groupId `inventory-service-group`
- `KafkaEmailConsumer` → `@KafkaListener` with groupId `notification-service-group`
- `OrderInventory` → JPA entity saved to PostgreSQL table `order_inventory`
- `OrderInventoryRepository` → `JpaRepository<OrderInventory, Long>`

#### Domain Model (base-domain, package: com.basedomain):
```
OrderDTO:       orderId, customerId, productId, productName, quantity, price, status, createdAt
OrderEventDTO:  eventId, eventType, order (OrderDTO), message, status, createdAt
OrderStatus:    PENDING, CONFIRMED, PROCESSING, SHIPPED, DELIVERED, CANCELLED, FAILED
OrderEventType: ORDER_PLACED, ORDER_CONFIRMED, ORDER_CANCELLED, ORDER_FAILED
```

#### Infrastructure:
| Tool | Port | Purpose |
|---|---|---|
| Kafka (cp-kafka:7.6.0) | 9092 | Message broker |
| Zookeeper | 2181 | Kafka coordinator |
| Kafka UI | 8080 | Topic browser |
| Elasticsearch 8.15.3 | 9200 | Log storage |
| Logstash 8.15.3 | 5000 | Log pipeline |
| Kibana 8.15.3 | 5601 | Log dashboard |
| Zipkin 3.4 | 9411 | Distributed tracing |
| PostgreSQL 15 | 5432 | Inventory DB (postgres/postgres, db: inventorydb) |

#### Observability:
- `logback-spring.xml` in each service — `LogstashTcpSocketAppender` → JSON logs → Logstash → Elasticsearch
- Kibana Data View: `spring-logs-*` — one index per service per day
- Zipkin tracing: `management.tracing.sampling.probability: 1.0`
- `service_name` custom field in logback used for Kibana filtering

#### Bugs fixed during session (important to remember):
1. YAML indentation — `kafka`, `datasource`, `jpa` were nested under `server:` instead of `spring:` — caused `spring.kafka.topic.name` not to resolve
2. `groupId = "${spring.kafka.consumer.group-id}"` in `@KafkaListener` doesn't resolve from Spring Boot auto-config — use hardcoded string or direct property key
3. `KafkaTopicConfig` missing — `NewTopic` bean must exist for `OrderKafkaProducer` to inject it
4. notification-service `logback-spring.xml` had `defaultValue="email-service"` — logs appeared under wrong Kibana index

#### Preferences learned:
- Separate `pom.xml` per service — NO shared parent pom between microservices
- Package: `com.mateen.{servicename}` for services, `com.basedomain` for base-domain
- `application.yml` with section comments
- Lombok: `@Data @Builder @NoArgsConstructor @AllArgsConstructor`
- Logger: `private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);`
- PostgreSQL: `postgres`/`postgres`, DB name matches service
- Java 17 — never use 21

---

## What's Missing — Advanced Kafka Concepts to Learn

| # | Concept | What it solves |
|---|---|---|
| 1 | **Dead Letter Topic (DLT)** | Where failed messages go after retries exhausted |
| 2 | **Retry Mechanism** | Auto-retry failed consumer messages before DLT |
| 3 | **Idempotent Producer** | Prevents duplicate messages on producer retry |
| 4 | **Kafka Transactions** | Exactly-once — message processed AND saved or neither |
| 5 | **Consumer Group Rebalancing** | What happens when services scale up/down |
| 6 | **Partitioning by Key** | Order guaranteed per partition, routing by key |
| 7 | **Kafka Streams** | Real-time stream processing inside Kafka |
| 8 | **Schema Registry + Avro** | Enforce message structure, replace JSON in production |
| 9 | **Kafka Connect** | Move data between Kafka and DBs/S3/ES without code |
| 10 | **Error Handling** | Global error handler, custom retry logic |
| 11 | **Consumer Lag Monitoring** | How far behind is your consumer from producer |
| 12 | **Kafka Security (SSL/SASL)** | Encryption and authentication for production |

---

## Vision for kafka-advanced

**Goal:** One project that covers ALL the above concepts in a real-world e-commerce order processing system. Each concept is implemented in a specific service so it makes practical sense.

### Architecture:
```
[Order REST API]
       │
       ▼
order-service  ──────────────────────────────────▶  [ order-events topic ]
(Idempotent Producer + Partitioning by customerId)          │
                                        ┌───────────────────┼───────────────┐
                                        ▼                   ▼               ▼
                                payment-service    shipping-service    audit-service
                                (Retry 3x + DLT)  (Retry 3x + DLT)  (Kafka Streams)
                                        │
                                        ▼
                                [ order-events-dlt topic ]
                                        │
                                        ▼
                                dlt-handler-service
                                (alerts on failures)
```

---

## Build Plan — Phase by Phase

### Phase 1 — Practical (Start Here)
| Step | Concept | Service |
|---|---|---|
| 1 | Idempotent Producer | order-service |
| 2 | Partitioning by Key (customerId) | order-service |
| 3 | Retry Mechanism (3 attempts, backoff) | payment-service |
| 4 | Dead Letter Topic | payment-service → order-events-dlt |
| 5 | Global Error Handler | payment-service |

### Phase 2 — Intermediate
| Step | Concept | Service |
|---|---|---|
| 6 | Kafka Streams (real-time order count) | audit-service |
| 7 | Consumer Lag Monitoring | Actuator + Kafka UI |
| 8 | Schema Registry + Avro | all services |

### Phase 3 — Production
| Step | Concept | Service |
|---|---|---|
| 9  | Kafka Transactions (exactly-once) | order-service + payment-service |
| 10 | Kafka Connect | DB sink connector |
| 11 | SSL/SASL Security | broker + all services |

---

## How to Start This Session

Open Claude Code in this folder and say:

> "Read CLAUDE.md and let's start building kafka-advanced Phase 1"

Claude will have full context and will start with:
1. Creating `order-service` pom.xml with idempotent producer config
2. Creating `payment-service` with retry + DLT
3. No need to explain anything from scratch
