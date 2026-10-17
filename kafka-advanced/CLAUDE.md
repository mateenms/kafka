# kafka-advanced — Session Context for Claude

## Who is the user
- Name: Mohammad Mateen (javamateen@gmail.com)
- Java 17 on local machine
- Experience: Built event-driven Kafka microservices project (see kafka/event-driven-kafka)
- Comfortable with: Spring Boot 3, Kafka producer/consumer, ELK, Zipkin, Docker Compose

---

## What was built before (existing project)

Located at: `/Users/mohammadmateen/kafka/event-driven-kafka/`

GitHub: https://github.com/mateenms/kafka

### Services built:
- `order-service` (port 8081) — REST API, Kafka producer, publishes to `order_topic`
- `inventory-service` (port 8083) — Kafka consumer, saves to PostgreSQL
- `notification-service` (port 8082) — Kafka consumer, logs email notifications
- `base-domain` — shared DTOs: OrderDTO, OrderEventDTO, enums

### Infrastructure used:
- Kafka (cp-kafka:7.6.0) on port 9092
- Zookeeper (cp-zookeeper:7.6.0)
- Kafka UI (provectuslabs/kafka-ui) on port 8080
- Elasticsearch 8.15.3 on port 9200
- Logstash 8.15.3 on port 5000
- Kibana 8.15.3 on port 5601
- Zipkin 3.4 on port 9411
- PostgreSQL 15 on port 5432 (postgres/postgres, db: inventorydb)

### Tech stack:
- Spring Boot 3.3.4, Java 17
- spring-kafka, LogstashEncoder 7.4
- Micrometer Tracing Brave + zipkin-reporter-brave
- Lombok, MapStruct 1.6.2
- Spring Data JPA + Hibernate
- Docker Compose

---

## Why this project exists

The existing project covered Kafka basics (producer, consumer, JSON serialization, ELK, Zipkin).
This `kafka-advanced` project is meant to cover the **missing production-grade Kafka concepts**.

---

## What to build here — kafka-advanced

### Goal
One standalone project covering all advanced Kafka concepts in a real-world e-commerce context.

### Architecture
```
[Order API]
     │
     ▼
order-producer  ──────────────────────────▶  [ order-events topic ]
(Idempotent + Transactional)                        │
                                    ┌───────────────┼───────────────┐
                                    ▼               ▼               ▼
                            payment-service  shipping-service  audit-service
                            (Retry + DLT)    (Retry + DLT)    (Kafka Streams)
                                    │
                                    ▼
                            [ dead-letter topic ]
                                    │
                                    ▼
                            dlt-handler-service
```

---

## Build Plan

### Phase 1 — Start here (Practical)
| # | Concept | Where to implement |
|---|---|---|
| 1 | **Dead Letter Topic (DLT)** | payment-service consumer |
| 2 | **Retry Mechanism** | payment-service — retry 3x before DLT |
| 3 | **Idempotent Producer** | order-producer config |
| 4 | **Error Handling** | Global Kafka error handler |
| 5 | **Partitioning by Key** | Route by customerId so order is preserved |

### Phase 2 — Intermediate
| # | Concept | Where to implement |
|---|---|---|
| 6 | **Kafka Streams** | audit-service — real-time order count aggregation |
| 7 | **Consumer Lag Monitoring** | Actuator + Kafka UI metrics |
| 8 | **Schema Registry + Avro** | Replace JSON serialization with Avro |

### Phase 3 — Production
| # | Concept | Where to implement |
|---|---|---|
| 9  | **Kafka Transactions** | Exactly-once — produce + DB save atomically |
| 10 | **Kafka Connect** | Sink connector to push events to PostgreSQL |
| 11 | **SSL/SASL Security** | Broker + client authentication |

---

## How to start the session

When opening Claude Code in this folder, say:

> "Read CLAUDE.md and let's start building kafka-advanced Phase 1"

Claude will read this file and have full context of:
- What was built before
- What needs to be built now
- The architecture and plan

---

## Preferences (from previous sessions)
- Java 17 (not 21)
- Separate pom.xml per service (no parent pom)
- Port convention: 8081, 8082, 8083...
- Package: com.mateen.{servicename}
- Shared DTOs in base-domain with package com.basedomain
- application.yml with comments
- Lombok (@Data, @Builder, @RequiredArgsConstructor)
- Logger: LoggerFactory.getLogger(ClassName.class)
- Docker Compose for infrastructure
- PostgreSQL credentials: postgres/postgres
