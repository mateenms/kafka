package com.mateen.notificationservice.kafka;

import com.basedomain.dto.OrderEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaEmailConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEmailConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "notification-service-group")
    public void consume(OrderEventDTO orderEventDTO) {
        LOGGER.info("===== Notification Service: New Kafka Event Received =====");
        LOGGER.info("Event ID    : {}", orderEventDTO.getEventId());
        LOGGER.info("Event Type  : {}", orderEventDTO.getEventType());
        LOGGER.info("Event Time  : {}", orderEventDTO.getCreatedAt());
        LOGGER.info("------ Order Details ------");
        LOGGER.info("Order ID    : {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("Customer ID : {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.info("Product ID  : {}", orderEventDTO.getOrder().getProductId());
        LOGGER.info("Product Name: {}", orderEventDTO.getOrder().getProductName());
        LOGGER.info("Quantity    : {}", orderEventDTO.getOrder().getQuantity());
        LOGGER.info("Price       : {}", orderEventDTO.getOrder().getPrice());
        LOGGER.info("Status      : {}", orderEventDTO.getOrder().getStatus());
        LOGGER.info("Order Time  : {}", orderEventDTO.getOrder().getCreatedAt());
        LOGGER.info("------ Sending Notification ------");
        LOGGER.info("Sending email notification to customer: {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.info("Notification sent successfully for order: {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("===== Notification Processing Complete =====");
    }
}
