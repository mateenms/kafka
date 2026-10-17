# Wikimedia Data Producer

A Spring Boot application that streams real-time Wikipedia edit events from the
[Wikimedia EventStreams API](https://stream.wikimedia.org/) and publishes them to
an Apache Kafka topic.

---

## What This Project Does

Wikipedia gets edited thousands of times every minute. Wikimedia publishes every
single one of those edits as a live stream that anyone can connect to. This project
**reads that live stream and saves each edit event into Kafka** so other applications
can process it later.

Think of it as a data pipe:

```
Wikipedia edits  →  this app  →  Kafka
```

This is the **first half** of a larger data pipeline. The second half would be a
Kafka **consumer** — a separate application that reads those stored messages and does
something useful with them (count edits per user, detect bots, build dashboards, etc.).

---

## Understanding the Three Pieces

### 1. The Data Source — Wikimedia EventStreams

Wikimedia runs a public URL that broadcasts a continuous stream of events:

```
https://stream.wikimedia.org/v2/stream/recentchange
```

Every time someone edits a Wikipedia page, that URL pushes a JSON message to
anyone listening. This is called **SSE (Server-Sent Events)** — it works like a
radio station: you tune in once and it keeps broadcasting to you non-stop.

Each event looks like this:

```json
{
  "type": "edit",
  "wiki": "enwiki",
  "title": "Python (programming language)",
  "user": "SomeEditor",
  "timestamp": 1729033200,
  "bot": false
}
```

### 2. This App — the Producer

The app connects to the Wikimedia URL, listens to the stream, and for every event
it receives it pushes the JSON into Kafka. Two classes share this responsibility:

- **`WikimediaChangesProducer`** — opens the connection to Wikimedia, sets the
  required User-Agent header, and keeps the connection alive for the configured
  duration.
- **`WikimediaChangesHandler`** — receives each event that arrives from the stream
  and hands it to Kafka via `KafkaTemplate`.

### 3. The Destination — Apache Kafka

Kafka is a message queue (also called a message broker). Think of it like a
**logbook**: every edit event gets written into a "chapter" called a **topic**
(`wikimedia-recent-change`). Other applications (consumers) can read from that
topic whenever they want, in order, at their own pace — even hours later.

---

## Step-by-Step Flow

```
Step 1:  Spring Boot starts — application context loads all beans.

Step 2:  KafkaTopicConfig tells Kafka to create the topic
         "wikimedia-recent-change" if it does not exist yet.

Step 3:  CommandLineRunner calls WikimediaChangesProducer.startStream().

Step 4:  WikimediaChangesProducer opens an HTTP connection to
         stream.wikimedia.org and sends a User-Agent header.

Step 5:  Wikimedia starts pushing JSON events down that connection —
         one JSON object per Wikipedia edit, continuously.

Step 6:  WikimediaChangesHandler receives each event and calls
         kafkaTemplate.send("wikimedia-recent-change", json).

Step 7:  Kafka stores the message in the topic — it is now available
         for any consumer application to read.

Step 8:  After 1 minute the stream closes and the app shuts down
         gracefully, flushing any pending Kafka messages first.
```

---

## Why the User-Agent Matters

Wikimedia's policy says: identify yourself or we block you. The `User-Agent` HTTP
header is how you identify your application. When it contains a placeholder value
(`set-a-real-url-or-email`), Wikimedia returns **HTTP 403 Forbidden**. With a real
email address in the header, Wikimedia allows the connection.

**Always set a real email in `application.yaml` before running this project.**

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                  Wikimedia EventStreams API                   │
│  https://stream.wikimedia.org/v2/stream/recentchange         │
│  (broadcasts a JSON event for every Wikipedia edit)          │
└──────────────────────────┬───────────────────────────────────┘
                           │  SSE — HTTP long-lived connection
                           │  pushes JSON events continuously
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                WikimediaChangesProducer                       │
│  • Opens BackgroundEventSource (non-blocking SSE client)     │
│  • Sends required User-Agent header (your email)             │
│  • Keeps stream open for STREAM_DURATION_MINUTES             │
└──────────────────────────┬───────────────────────────────────┘
                           │  onMessage(event, jsonPayload)
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                WikimediaChangesHandler                        │
│  • onOpen()    → logs "Connected"                            │
│  • onMessage() → calls kafkaTemplate.send(topic, json)       │
│  • onError()   → logs the error (client auto-retries)        │
│  • onClosed()  → logs "Disconnected"                         │
└──────────────────────────┬───────────────────────────────────┘
                           │  produce record
                           │  key=null, value=JSON string
                           ▼
┌──────────────────────────────────────────────────────────────┐
│               Apache Kafka  (localhost:9094)                  │
│               Topic: wikimedia-recent-change                  │
│               1 partition · 1 replica · idempotent producer  │
└──────────────────────────────────────────────────────────────┘
```

---

## Package Layout

```
com.ms.wikimedia_data_producer
├── WikimediaDataProducerApplication.java   Entry point; implements CommandLineRunner
├── config/
│   └── KafkaTopicConfig.java               Declares the Kafka topic bean
└── stream/
    ├── WikimediaChangesProducer.java        Opens and manages the SSE stream
    └── WikimediaChangesHandler.java         SSE callbacks → Kafka
```

| Class | Role |
|---|---|
| `WikimediaDataProducerApplication` | Spring Boot entry point. `CommandLineRunner.run()` calls `startStream()` immediately after the context starts. |
| `KafkaTopicConfig` | Declares a `NewTopic` bean so `KafkaAdmin` auto-creates the topic on startup if missing. |
| `WikimediaChangesProducer` | Builds a `BackgroundEventSource`, connects to the SSE endpoint, and blocks the runner thread for the configured duration. |
| `WikimediaChangesHandler` | Receives every SSE lifecycle callback. On `onMessage` it publishes the raw JSON payload to Kafka via `KafkaTemplate`. |

---

## Tech Stack

| Component | Version |
|---|---|
| Java | 17 |
| Spring Boot | 4.1.1 |
| Spring Kafka | 4.1.1 |
| Apache Kafka Client | 4.2.1 |
| LaunchDarkly okhttp-eventsource | 4.3.0 |
| Lombok | 1.18.x |

---

## Prerequisites

- Java 17+
- Maven 3.8+
- A running Kafka broker on `localhost:9094`

### Start Kafka (Docker)

```bash
docker run -d \
  --name kafka \
  -p 9094:9094 \
  apache/kafka:latest
```

Or if you use a `docker-compose.yml` in the parent project:

```bash
docker compose up -d
```

---

## Configuration

All settings live in [`src/main/resources/application.yaml`](src/main/resources/application.yaml).

| Property | Default | Description |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `localhost:9094` | Kafka broker address |
| `wikimedia.stream.url` | `https://stream.wikimedia.org/v2/stream/recentchange` | Wikimedia SSE endpoint |
| `wikimedia.stream.user-agent` | `wikimedia-data-producer/0.0.1 (contact: …)` | HTTP User-Agent — **must contain a real email or project URL** |
| `wikimedia.kafka.topic` | `wikimedia-recent-change` | Kafka topic to publish events to |
| `server.port` | `8081` | Embedded Tomcat port |

> **Important:** Wikimedia blocks requests with a generic or placeholder User-Agent.
> Replace the `contact:` value with your actual email address before running.

---

## How to Run

### 1. Build the project

```bash
mvn clean package -DskipTests
```

### 2. Run in development (live reload)

```bash
mvn spring-boot:run
```

### 3. Run the packaged JAR

```bash
java -jar target/wikimedia-data-producer-0.0.1-SNAPSHOT.jar
```

### 4. Run tests

```bash
mvn test
```

---

## Verify Events Are Arriving in Kafka

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic wikimedia-recent-change \
  --from-beginning
```

You should see a continuous stream of JSON objects — one per Wikipedia edit.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `HTTP error 403` from Wikimedia | Placeholder User-Agent | Set a real email in `wikimedia.stream.user-agent` |
| `Connection refused` to Kafka | Broker not running | Start Kafka on `localhost:9094` |
| Topic not created | Broker unreachable at startup | Ensure broker is up before starting the app |
| Stream disconnects and reconnects | Transient network or rate-limit | Expected — the library retries automatically |
