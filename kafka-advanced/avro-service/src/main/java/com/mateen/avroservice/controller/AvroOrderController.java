package com.mateen.avroservice.controller;

import com.mateen.avroservice.kafka.AvroOrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/avro/orders")
public class AvroOrderController {

    private final AvroOrderProducer avroOrderProducer;

    public AvroOrderController(AvroOrderProducer avroOrderProducer) {
        this.avroOrderProducer = avroOrderProducer;
    }

    /**
     * POST /api/avro/orders
     *
     * Sends an Avro-serialized OrderEvent to the avro-order-events topic.
     * The message is serialized as binary (not JSON) using the Avro schema.
     * The schema is registered in Confluent Schema Registry on first send.
     *
     * Request body:
     * {
     *   "customerId": "CUST001",
     *   "productName": "Laptop",
     *   "quantity": 1,
     *   "price": "1299.99"
     * }
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> sendAvroOrder(@RequestBody Map<String, String> request) {
        String customerId = request.get("customerId");
        String productName = request.get("productName");
        int quantity = Integer.parseInt(request.getOrDefault("quantity", "1"));
        String price = request.getOrDefault("price", "0.00");

        avroOrderProducer.sendOrderEvent(customerId, productName, quantity, price);

        return ResponseEntity.ok(Map.of(
                "status", "sent",
                "format", "avro",
                "customerId", customerId,
                "productName", productName,
                "schemaRegistry", "http://localhost:8091",
                "message", "Avro message sent — check avro-order-events topic in Kafka UI"
        ));
    }
}
