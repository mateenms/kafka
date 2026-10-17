# Architecture — Event-Driven Kafka Microservices

## High-Level Architecture

```
                          ┌────────────────────────────────────────────────────────────┐
                          │                    MICROSERVICES LAYER                     │
                          └────────────────────────────────────────────────────────────┘

     ┌──────────────┐       POST /api/v1/order/orders
     │    Client    │ ─────────────────────────────────────▶ ┌──────────────────────┐
     │  (Postman)   │                                         │    order-service     │
     └──────────────┘                                         │      Port: 8081      │
                                                              │                      │
                                                              │  OrderController     │
                                                              │  OrderKafkaProducer  │
                                                              │  KafkaTopicConfig    │
                                                              └──────────┬───────────┘
                                                                         │
                                                                publishes event
                                                                         │
                                                                         ▼
                                                          ┌──────────────────────────┐
                                                          │    Apache Kafka          │
                                                          │    Topic: order_topic    │
                                                          │    Partitions: 3         │
                                                          └─────────────┬────────────┘
                                                                        │
                              ┌─────────────────────────────────────────┤
                              │                                         │
                              ▼                                         ▼
              ┌───────────────────────────┐           ┌─────────────────────────────┐
              │     inventory-service     │           │     notification-service    │
              │        Port: 8083         │           │          Port: 8082         │
              │                           │           │                             │
              │  group: inventory-service │           │  group: notification-service│
              │  OrderConsumer.consume()  │           │  KafkaEmailConsumer         │
              │                           │           │       .consume()            │
              └──────────────┬────────────┘           └─────────────────────────────┘
                             │
                             ▼
              ┌──────────────────────────┐
              │      PostgreSQL DB       │
              │   Table: order_inventory │
              │       Port: 5432         │
              └──────────────────────────┘
```

---

## Observability Layer

```
  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │order-service │    │  inventory   │    │notification  │
  │  Port: 8081  │    │  Port: 8083  │    │  Port: 8082  │
  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
         │                   │                   │
         │   JSON logs via Logstash TCP (5000)   │
         └───────────────────┼───────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Logstash     │
                    │   Port: 5000    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Elasticsearch   │     Index: spring-logs-{service}-{date}
                    │   Port: 9200    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Kibana      │     http://localhost:5601
                    │   Port: 5601    │     Data View: spring-logs-*
                    └─────────────────┘

         │  HTTP trace via Micrometer Brave    │
         └───────────────────┬─────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Zipkin      │     http://localhost:9411
                    │   Port: 9411    │
                    └─────────────────┘
```

---

## Data Flow — Order Placed Event

```
Step 1:  Client sends POST request with OrderDTO (customerId, productId, price, quantity)

Step 2:  order-service
         ├── Sets orderId    = UUID
         ├── Sets status     = PENDING
         ├── Sets createdAt  = now()
         ├── Wraps in OrderEventDTO (eventId, eventType=ORDER_PLACED, order)
         └── Publishes to Kafka topic: order_topic

Step 3:  Kafka stores message in order_topic (3 partitions)

Step 4:  inventory-service (group: inventory-service-group)
         ├── Consumes OrderEventDTO from order_topic
         ├── Logs all event + order details
         └── Saves OrderInventory entity to PostgreSQL (inventorydb)

Step 5:  notification-service (group: notification-service-group)
         ├── Consumes OrderEventDTO from order_topic
         └── Logs all event + order details → triggers email notification

Step 6:  All services send structured JSON logs → Logstash → Elasticsearch → Kibana

Step 7:  All HTTP requests are traced → Zipkin (100% sampling in dev)
```

---

## Domain Model (base-domain)

```
OrderDTO
├── orderId       String
├── customerId    String
├── productId     String
├── productName   String
├── quantity      Integer
├── price         BigDecimal
├── status        OrderStatus  (PENDING | CONFIRMED | PROCESSING | SHIPPED | DELIVERED | CANCELLED | FAILED)
└── createdAt     LocalDateTime

OrderEventDTO
├── eventId       String
├── eventType     OrderEventType  (ORDER_PLACED | ORDER_CONFIRMED | ORDER_CANCELLED | ORDER_FAILED)
├── order         OrderDTO
├── message       String
├── status        String
└── createdAt     LocalDateTime
```

---

## Kafka Configuration

```
Topic         : order_topic
Partitions    : 3
Replicas      : 1
Serializer    : JsonSerializer   (producer)
Deserializer  : JsonDeserializer (consumer)
Trusted pkg   : * (all packages allowed for deserialization)

Producer group: order-service
Consumer groups:
  ├── inventory-service-group    (inventory-service)
  └── notification-service-group (notification-service)
```

---

## Infrastructure Ports Reference

```
order-service          → 8081
notification-service   → 8082
inventory-service      → 8083
Kafka                  → 9092
Zookeeper              → 2181
Kafka UI               → 8080
Elasticsearch          → 9200
Logstash               → 5000
Kibana                 → 5601
Zipkin                 → 9411
PostgreSQL             → 5432
```
