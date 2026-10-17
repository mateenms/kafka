# Wikimedia Data Consumer

A Spring Boot application that reads Wikimedia recent-change events from a
Kafka topic and persists each event to a PostgreSQL database.

---

## What This Project Does

This is the **second half** of the Wikimedia Kafka pipeline. It continuously
reads JSON messages that the producer wrote into Kafka and saves each one as
a row in PostgreSQL so the data can be queried, analysed, or visualised later.

```
Kafka (wikimedia-recent-change)  →  this app  →  PostgreSQL (mydb)
```

---

## Step-by-Step Flow

```
Step 1:  Spring Boot starts on port 8083.

Step 2:  Hibernate connects to PostgreSQL (mydb) and auto-creates
         the wikimedia_data table if it does not exist.

Step 3:  @KafkaListener in WikimediaChangesConsumer subscribes to
         the topic wikimedia-recent-change in group wikimedia-consumer-group.

Step 4:  For every message on the topic, consume() is called with
         the raw JSON string.

Step 5:  A new WikiEventData entity is created, eventData is set
         to the JSON string, and repository.save() persists it.

Step 6:  @PrePersist sets createdAt to the current timestamp
         automatically before each INSERT.

Step 7:  Spring Kafka commits the offset after save() returns,
         so the message is not reprocessed on restart.
```

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│               Apache Kafka  (localhost:9094)                  │
│               Topic: wikimedia-recent-change                  │
└──────────────────────────┬───────────────────────────────────┘
                           │  @KafkaListener
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                WikimediaChangesConsumer                       │
│  • Receives raw JSON message from Kafka                      │
│  • Creates a WikiEventData entity                            │
│  • Calls repository.save(wikiEventData)                      │
└──────────────────────────┬───────────────────────────────────┘
                           │  JPA → INSERT
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                WikimediaDataRepository                        │
│  • extends JpaRepository<WikiEventData, Long>                │
│  • save() / findAll() / findById() out of the box            │
└──────────────────────────┬───────────────────────────────────┘
                           │  Hibernate SQL
                           ▼
┌──────────────────────────────────────────────────────────────┐
│               PostgreSQL  (localhost:5432)                    │
│               Database: mydb                                 │
│               Table:    wikimedia_data                       │
│               Columns:  id · event_data (TEXT) · created_at  │
└──────────────────────────────────────────────────────────────┘
```

---

## Package Layout

```
com.ms.wikimedia_data_consumer
├── WikimediaDataConsumerApplication.java   entry point
├── entity/
│   └── WikiEventData.java                  JPA entity → table wikimedia_data
├── repository/
│   └── WikimediaDataRepository.java        extends JpaRepository
└── consumer/
    └── WikimediaChangesConsumer.java        @KafkaListener → repository.save()
```

| Class | Role |
|---|---|
| `WikimediaDataConsumerApplication` | Spring Boot entry point |
| `WikiEventData` | JPA entity with `id`, `eventData` (`@Lob`), `createdAt` |
| `WikimediaDataRepository` | Spring Data repository — provides save/find/delete |
| `WikimediaChangesConsumer` | Listens to Kafka, saves each event via the repository |

---

## Tech Stack

| Component | Version |
|---|---|
| Java | 17 |
| Spring Boot | 4.1.1 |
| Spring Kafka | 4.1.1 |
| Spring Data JPA | 4.1.1 |
| Hibernate | (managed by Spring Boot) |
| PostgreSQL Driver | latest |
| Lombok | 1.18.x |

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Kafka broker running on `localhost:9094`
- PostgreSQL running on `localhost:5432` with database `mydb`

---

## Configuration

All settings in [`src/main/resources/application.yaml`](src/main/resources/application.yaml).

| Property | Value | Description |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `localhost:9094` | Kafka broker address |
| `spring.kafka.consumer.group-id` | `wikimedia-consumer-group` | Consumer group ID |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | Read from beginning on first start |
| `wikimedia.kafka.topic` | `wikimedia-recent-change` | Topic to consume from |
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/mydb` | PostgreSQL connection |
| `spring.datasource.username` | `postgres` | DB username |
| `spring.datasource.password` | `postgres` | DB password |
| `spring.jpa.hibernate.ddl-auto` | `create-drop` | Auto-creates table on startup (dev only) |
| `spring.jpa.show-sql` | `true` | Prints SQL to console |
| `server.port` | `8083` | Embedded Tomcat port |

---

## How to Run

```bash
mvn spring-boot:run
```

Or run `WikimediaDataConsumerApplication` directly from IntelliJ.

---

## Verify Events Are Being Saved

In DBeaver or psql:

```sql
SELECT id, created_at, LEFT(event_data, 120)
FROM wikimedia_data
ORDER BY created_at DESC
LIMIT 10;
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Connection refused` to PostgreSQL | DB not running | Start the postgres Docker container |
| `database "mydb" does not exist` | DB not created | `docker exec postgres psql -U postgres -c "CREATE DATABASE mydb;"` |
| No rows in table | Producer not running | Start `wikimedia-data-producer` first |
| `Table wikimedia_data does not exist` | JPA not initialised | Check `ddl-auto` is `create-drop` or `update` |
