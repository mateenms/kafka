# SSL/SASL Security — Kafka Authentication

## Overview

Kafka supports two types of security:

| | SSL | SASL |
|-|-----|------|
| Purpose | Encryption + optional authentication | Authentication (identity verification) |
| How | TLS certificates | Username/password or Kerberos tickets |
| Complexity | High (certificate management) | Low (PLAIN) to High (Kerberos) |
| Production use | SSL encryption + SASL/SCRAM auth together | Common combination |

This directory demonstrates **SASL/PLAIN** — the simplest form of authentication. Username/password, no certificates needed. Good for learning and internal environments.

---

## SASL Mechanisms Compared

| Mechanism | How | Security | Production ready |
|-----------|-----|----------|-----------------|
| **PLAIN** | Username + password in plaintext | Low (use with TLS!) | Only with SSL |
| **SCRAM-SHA-256** | Challenge/response hashing | Medium | Yes |
| **SCRAM-SHA-512** | Stronger hashing | High | Yes |
| **GSSAPI (Kerberos)** | Kerberos tickets | Very high | Enterprise |
| **OAUTHBEARER** | OAuth 2.0 tokens | High | Cloud-native |

In production: use **SASL/SCRAM-SHA-512 + SSL encryption**.
This demo uses **SASL/PLAIN** for simplicity (no certs, easy to understand).

---

## How SASL/PLAIN Works

```
Client (order-service)                   Kafka Broker
        │                                     │
        │  1. Connect to SASL_PLAINTEXT:9092  │
        │ ──────────────────────────────────▶ │
        │                                     │
        │  2. Send credentials:               │
        │     username="order-service"        │
        │     password="order-secret"         │
        │ ──────────────────────────────────▶ │
        │                                     │  3. Check against JAAS config
        │                                     │     (kafka_server_jaas.conf)
        │  4. AUTH SUCCESS / FAIL             │
        │ ◀────────────────────────────────── │
        │                                     │
        │  5. Start producing/consuming       │
        │ ──────────────────────────────────▶ │
```

---

## Files in This Directory

| File | Purpose |
|------|---------|
| `kafka_server_jaas.conf` | Broker's allowed users — mounted into Kafka container |
| `docker-compose-sasl.yml` | Full Kafka stack with SASL/PLAIN enabled |
| `application-sasl.yml` | Spring Boot config snippet for services to connect with SASL |

---

## How to Run SASL Demo

### Step 1 — Start the secured Kafka broker

```bash
cd /Users/mohammadmateen/kafka/kafka-advanced/security-config
docker-compose -f docker-compose-sasl.yml up -d
```

### Step 2 — Verify the broker requires auth

```bash
# Without auth — should fail or hang
kafka-topics.sh --bootstrap-server localhost:9092 --list

# With auth — should succeed
kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config /path/to/client.properties \
  --list
```

### Step 3 — Test with Kafka UI

Open `http://localhost:8080` — Kafka UI is pre-configured with admin credentials and will show topics.

---

## Applying SASL to Spring Boot Services

Add to your service's `application.yml` (see `application-sasl.yml` for full template):

```yaml
spring:
  kafka:
    producer:
      properties:
        security.protocol: SASL_PLAINTEXT
        sasl.mechanism: PLAIN
        sasl.jaas.config: >
          org.apache.kafka.common.security.plain.PlainLoginModule required
          username="order-service"
          password="order-secret";
    consumer:
      properties:
        security.protocol: SASL_PLAINTEXT
        sasl.mechanism: PLAIN
        sasl.jaas.config: >
          org.apache.kafka.common.security.plain.PlainLoginModule required
          username="payment-service"
          password="payment-secret";
```

Each service authenticates with its own username/password. The broker checks them against `kafka_server_jaas.conf`.

---

## Adding SSL Encryption (Production)

SASL/PLAIN sends credentials in cleartext. In production, combine with SSL:

```
SASL_SSL instead of SASL_PLAINTEXT
```

Steps to add SSL:
1. Generate CA (Certificate Authority)
2. Generate broker keystore and truststore
3. Generate client truststore
4. Update Kafka listeners to `SASL_SSL`
5. Add SSL properties to services:

```yaml
spring:
  kafka:
    producer:
      properties:
        security.protocol: SASL_SSL
        sasl.mechanism: PLAIN
        sasl.jaas.config: "..."
        ssl.truststore.location: /certs/client.truststore.jks
        ssl.truststore.password: changeit
```

Certificate generation commands:
```bash
# 1. Create CA
openssl req -new -x509 -keyout ca.key -out ca.crt -days 365

# 2. Create broker keystore
keytool -genkey -alias broker -keyalg RSA \
  -keystore broker.keystore.jks -storepass changeit

# 3. Sign broker cert with CA
keytool -certreq -alias broker -keystore broker.keystore.jks \
  | openssl x509 -req -CA ca.crt -CAkey ca.key -out broker.crt

# 4. Import into keystore
keytool -importcert -alias ca -file ca.crt -keystore broker.keystore.jks
keytool -importcert -alias broker -file broker.crt -keystore broker.keystore.jks

# 5. Create client truststore
keytool -importcert -alias ca -file ca.crt -keystore client.truststore.jks
```

---

## kafka_server_jaas.conf — User Management

To add a new service user:

```
// In kafka_server_jaas.conf — add one line per user:
user_new-service="new-service-secret"
```

Then in the new service's application.yml:
```yaml
sasl.jaas.config: >
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="new-service"
  password="new-service-secret";
```

---

## Security Best Practices

| Practice | Why |
|----------|-----|
| Use SASL/SCRAM over SASL/PLAIN | Passwords aren't sent in cleartext |
| Always add SSL in production | Encrypts traffic between clients and broker |
| Rotate credentials regularly | Limits blast radius of a leaked credential |
| One credential per service | Easy to revoke one service without affecting others |
| Use ACLs to restrict topic access | Order-service should only produce to order-events, not consume |

---

## Phase in Project

This config is part of **Phase 3 — Production Kafka**:

- Phase 1 ✅ — Idempotent Producer, Partitioning, Retry + DLT, Global Error Handler
- Phase 2 ✅ — Kafka Streams, Consumer Lag Monitoring, Schema Registry + Avro
- **Phase 3 (this)** — Kafka Transactions, Kafka Connect, **SSL/SASL**
