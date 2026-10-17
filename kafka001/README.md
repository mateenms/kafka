# kafka001 — Spring Boot Kafka Basics

A beginner-friendly Spring Boot project demonstrating Kafka **String** and **JSON** messaging with a REST API.

---

## What It Does

- Publishes plain **String** messages to a Kafka topic via REST
- Publishes **JSON (User object)** messages to a Kafka topic via REST
- Consumes both String and JSON messages with `@KafkaListener`
- Two separate topics: `myTopic` (String) and `secondTopic` (JSON)

---

## Architecture

```
[REST Client]
     │
     ├── POST /api/v1/publish?message=hello   → KafkaProducer     → myTopic      → KafkaConsumer
     │
     └── POST /api/v1/publish/json            → JsonKafkaProducer → secondTopic  → JsonKafkaConsumer
                  { "firstName": "...", "lastName": "...", "email": "..." }
```

---

## Project Structure

```
kafka001/
└── src/main/java/com/ms/kafka01/
    ├── config/
    │   ├── KafkaTopicConfig.java     # Creates Kafka topics
    │   └── JacksonConfig.java        # JSON serialization config
    ├── producer/
    │   ├── KafkaProducer.java        # Sends String messages
    │   └── JsonKafkaProducer.java    # Sends JSON (User) messages
    ├── consumer/
    │   ├── KafkaConsumer.java        # Consumes String messages
    │   └── JsonKafkaConsumer.java    # Consumes JSON (User) messages
    ├── controller/
    │   └── KafkaController.java      # REST endpoints
    └── payload/
        └── User.java                 # JSON payload model
```

---

## Tech Stack

| Component       | Version |
|-----------------|---------|
| Java            | 17      |
| Spring Boot     | 4.1.1   |
| Spring Kafka    | 4.1.1   |
| Lombok          | latest  |
| Jackson Databind| latest  |

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Desktop running

---

## How to Run

### Step 1 — Start Kafka

```bash
docker run -d --name kafka -p 9094:9094 apache/kafka:latest
```

### Step 2 — Build and run

```bash
mvn clean install
java -jar target/kafka01-0.0.1-SNAPSHOT.jar
```

Service starts on **http://localhost:8081**

---

## Test with Postman or curl

**Send a String message:**
```bash
curl -X POST "http://localhost:8081/api/v1/publish?message=HelloKafka"
```

**Send a JSON (User) message:**
```bash
curl -X POST http://localhost:8081/api/v1/publish/json \
  -H "Content-Type: application/json" \
  -d '{"firstName": "John", "lastName": "Doe", "email": "john@example.com"}'
```

Check the console — consumed messages will be printed by the `@KafkaListener` methods.

---

## Kafka Topics

| Topic       | Type   | Producer            | Consumer            |
|-------------|--------|---------------------|---------------------|
| myTopic     | String | KafkaProducer       | KafkaConsumer       |
| secondTopic | JSON   | JsonKafkaProducer   | JsonKafkaConsumer   |
