# kafka02-wikimedia — Full Pipeline

End-to-end Kafka pipeline that streams real-time Wikipedia edit events into
PostgreSQL via Apache Kafka.

```
Wikimedia API  →  Producer  →  Kafka  →  Consumer  →  PostgreSQL
```

---

## Projects

| Project | Port | Role |
|---|---|---|
| `wikimedia-data-producer` | `8081` | Reads the Wikimedia SSE stream, publishes each edit event to Kafka |
| `wikimedia-data-consumer` | `8083` | Reads from Kafka, persists each event to PostgreSQL |

---

## Full Pipeline Architecture

```
┌──────────────────────────────────────────────────┐
│           Wikimedia EventStreams API              │
│  https://stream.wikimedia.org/v2/stream/recentchange │
└─────────────────────┬────────────────────────────┘
                      │  SSE (Server-Sent Events)
                      ▼
┌──────────────────────────────────────────────────┐
│          wikimedia-data-producer  :8081           │
│  WikimediaChangesProducer                        │
│  WikimediaChangesHandler → KafkaTemplate         │
└─────────────────────┬────────────────────────────┘
                      │  produce (key=null, value=JSON)
                      ▼
┌──────────────────────────────────────────────────┐
│          Apache Kafka  (localhost:9094)           │
│          Topic: wikimedia-recent-change           │
└─────────────────────┬────────────────────────────┘
                      │  @KafkaListener
                      ▼
┌──────────────────────────────────────────────────┐
│          wikimedia-data-consumer  :8083           │
│  WikimediaChangesConsumer                        │
│  WikimediaDataRepository → PostgreSQL            │
└─────────────────────┬────────────────────────────┘
                      │  INSERT
                      ▼
┌──────────────────────────────────────────────────┐
│          PostgreSQL  (localhost:5432)             │
│          Database: mydb                          │
│          Table:    wikimedia_data                │
└──────────────────────────────────────────────────┘
```

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker (for Kafka and PostgreSQL)

### Start Kafka

```bash
docker run -d --name kafka -p 9094:9094 apache/kafka:latest
```

### Start PostgreSQL

```bash
docker run -d --name postgres -p 5432:5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  postgres:latest
```

### Create the database

```bash
docker exec postgres psql -U postgres -c "CREATE DATABASE mydb;"
```

---

## How to Run

Start in this order:

```bash
# 1. Start Kafka and PostgreSQL (Docker)
docker compose up -d

# 2. Run the producer (streams Wikimedia events into Kafka)
cd wikimedia-data-producer
mvn spring-boot:run

# 3. Run the consumer (reads from Kafka, saves to PostgreSQL)
cd wikimedia-data-consumer
mvn spring-boot:run
```

---

## Verify the Pipeline

Check events are saved in PostgreSQL:

```sql
SELECT id, created_at, LEFT(event_data, 100)
FROM wikimedia_data
ORDER BY created_at DESC
LIMIT 10;
```

---

## Tech Stack

| Component | Version |
|---|---|
| Java | 17 |
| Spring Boot | 4.1.1 |
| Spring Kafka | 4.1.1 |
| Apache Kafka Client | 4.2.1 |
| Spring Data JPA | 4.1.1 |
| PostgreSQL Driver | latest |
| LaunchDarkly okhttp-eventsource | 4.3.0 |
| Lombok | 1.18.x |
