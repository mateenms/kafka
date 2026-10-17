package com.mateen.avroservice.kafka;

import com.mateen.avro.OrderEvent;
import com.mateen.avro.OrderEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class AvroOrderProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroOrderProducer.class);

    @Value("${spring.kafka.topic.avro-order-events}")
    private String topic;

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public AvroOrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(String customerId, String productName, int quantity, String price) {

        // Build an Avro SpecificRecord — generated Java class from order-event.avsc
        // The schema was registered in Schema Registry when the first message was sent
        OrderEvent event = OrderEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(OrderEventType.ORDER_PLACED)
                .setOrderId(UUID.randomUUID().toString())
                .setCustomerId(customerId)
                .setProductName(productName)
                .setQuantity(quantity)
                .setPrice(price)
                .setStatus("PENDING")
                .setMessage(null)
                .setCreatedAt(LocalDateTime.now().toString())
                .build();

        LOGGER.info("Sending Avro OrderEvent — customerId: {} | product: {} | price: {}",
                customerId, productName, price);

        // Key = customerId (same partitioning strategy as order-service)
        // Value = Avro-serialized binary (not JSON)
        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(topic, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                LOGGER.info("Avro message sent — topic: {} | partition: {} | offset: {}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                LOGGER.error("Failed to send Avro message: {}", ex.getMessage());
            }
        });
    }
}
