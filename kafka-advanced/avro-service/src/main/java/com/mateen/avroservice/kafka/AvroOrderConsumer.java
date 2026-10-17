package com.mateen.avroservice.kafka;

import com.mateen.avro.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AvroOrderConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroOrderConsumer.class);

    // containerFactory must match the bean name in AvroKafkaConsumerConfig
    // Without this, Spring uses the default JsonDeserializer factory and the message fails to parse
    @KafkaListener(
            topics = "${spring.kafka.topic.avro-order-events}",
            groupId = "avro-service-group",
            containerFactory = "avroKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        String customerId = record.key();
        OrderEvent event = record.value();

        LOGGER.info("═══════════════════════════════════════════");
        LOGGER.info("Avro message received from Schema Registry");
        LOGGER.info("Partition key (customerId) : {}", customerId);
        LOGGER.info("Event ID                   : {}", event.getEventId());
        LOGGER.info("Event Type                 : {}", event.getEventType());
        LOGGER.info("Order ID                   : {}", event.getOrderId());
        LOGGER.info("Product                    : {}", event.getProductName());
        LOGGER.info("Quantity                   : {}", event.getQuantity());
        LOGGER.info("Price                      : {}", event.getPrice());
        LOGGER.info("Status                     : {}", event.getStatus());
        LOGGER.info("Message                    : {}", event.getMessage());
        LOGGER.info("Created At                 : {}", event.getCreatedAt());
        LOGGER.info("Topic                      : {}", record.topic());
        LOGGER.info("Partition                  : {}", record.partition());
        LOGGER.info("Offset                     : {}", record.offset());
        LOGGER.info("═══════════════════════════════════════════");
    }
}
