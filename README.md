# Kafka Learning Projects

A collection of Apache Kafka projects built with Spring Boot — from basics to production-ready event-driven microservices.

---

## Projects

| Project | Description | Level |
|---|---|---|
| [kafka001](./kafka001/) | Basic Kafka producer/consumer with String and JSON messages | Beginner |
| [kafka02-wikimedia](./kafka02-wikimedia/) | Real-time Wikimedia event stream → Kafka → PostgreSQL pipeline | Intermediate |
| [event-driven-kafka](./event-driven-kafka/) | Full event-driven microservices with ELK, Zipkin, PostgreSQL | Advanced |

---

## kafka001 — Kafka Basics

Spring Boot app with REST API to send and consume String and JSON messages over Kafka.

```
REST API → KafkaProducer → [ myTopic / secondTopic ] → KafkaConsumer
```

**Port:** 8081 | **Topics:** myTopic, secondTopic

→ [See README](./kafka001/README.md)

---

## kafka02-wikimedia — Real-Time Pipeline

Streams live Wikipedia edit events from Wikimedia API into PostgreSQL via Kafka.

```
Wikimedia SSE API → Producer → Kafka → Consumer → PostgreSQL
```

**Ports:** Producer: 8081 | Consumer: 8083

→ [See README](./kafka02-wikimedia/README.md)

---

## event-driven-kafka — Microservices Architecture

Production-style event-driven system with 3 microservices, ELK observability stack and distributed tracing.

```
Client → order-service → Kafka (order_topic) → inventory-service → PostgreSQL
                                              → notification-service
```

**Ports:** order: 8081 | notification: 8082 | inventory: 8083

**Observability:** Kibana (5601) | Zipkin (9411) | Kafka UI (8080)

→ [See README](./event-driven-kafka/README.md)

---

## Tech Stack (across all projects)

- **Java 17** + **Spring Boot 3/4**
- **Apache Kafka** + **Spring Kafka**
- **PostgreSQL** + **Spring Data JPA**
- **ELK Stack** (Elasticsearch, Logstash, Kibana)
- **Zipkin** distributed tracing
- **Docker** + **Docker Compose**
- **Lombok** + **MapStruct**
