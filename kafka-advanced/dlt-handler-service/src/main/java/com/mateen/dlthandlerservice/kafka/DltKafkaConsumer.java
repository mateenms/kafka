package com.mateen.dlthandlerservice.kafka;

import com.basedomain.dto.OrderEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class DltKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DltKafkaConsumer.class);

    // This service ONLY listens to the Dead Letter Topic
    // Messages arrive here after payment-service failed 3 retries
    // Purpose: alert ops team, create support ticket, trigger manual reprocessing
    @KafkaListener(topics = "${spring.kafka.topic.order-events-dlt}", groupId = "dlt-handler-group")
    public void consumeFromDlt(OrderEventDTO orderEventDTO,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               @Header(KafkaHeaders.OFFSET) long offset) {

        LOGGER.error("========================================");
        LOGGER.error("  DEAD LETTER TOPIC — FAILED MESSAGE   ");
        LOGGER.error("========================================");
        LOGGER.error("DLT Topic : {}", topic);
        LOGGER.error("Partition : {} | Offset: {}", partition, offset);
        LOGGER.error("------ Failed Order Details ------");
        LOGGER.error("Event ID  : {}", orderEventDTO.getEventId());
        LOGGER.error("Event Type: {}", orderEventDTO.getEventType());
        LOGGER.error("Order ID  : {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.error("Customer  : {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.error("Product   : {}", orderEventDTO.getOrder().getProductName());
        LOGGER.error("Amount    : {}", orderEventDTO.getOrder().getPrice());
        LOGGER.error("Status    : {}", orderEventDTO.getOrder().getStatus());
        LOGGER.error("------ Required Actions ------");
        LOGGER.error("1. Investigate root cause of payment failure");
        LOGGER.error("2. Notify customer: {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.error("3. Manually reprocess or refund order: {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.error("========================================");

        // In real-world:
        // - Send alert email to ops team
        // - Create ticket in Jira / PagerDuty
        // - Send failure notification to customer
        // - Store in DB for audit trail
    }
}
