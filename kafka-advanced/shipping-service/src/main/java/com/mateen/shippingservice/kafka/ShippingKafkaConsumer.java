package com.mateen.shippingservice.kafka;

import com.basedomain.dto.OrderEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class ShippingKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShippingKafkaConsumer.class);

    // Global error handler (KafkaErrorHandlerConfig) automatically applies to this listener
    // Any exception thrown here → retried 3 times → logged by error handler → message skipped
    @KafkaListener(topics = "${spring.kafka.topic.order-events}", groupId = "shipping-service-group")
    public void consume(OrderEventDTO orderEventDTO,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {

        LOGGER.info("===== Shipping Service: Order Event Received =====");
        LOGGER.info("Partition : {} | Offset: {}", partition, offset);
        LOGGER.info("Event ID  : {}", orderEventDTO.getEventId());
        LOGGER.info("Event Type: {}", orderEventDTO.getEventType());
        LOGGER.info("Order ID  : {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("Customer  : {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.info("Product   : {}", orderEventDTO.getOrder().getProductName());
        LOGGER.info("Quantity  : {}", orderEventDTO.getOrder().getQuantity());
        LOGGER.info("------ Processing Shipment ------");

        // Simulate shipment processing
        // In real-world: call shipping provider API (FedEx, DHL, etc.)
        LOGGER.info("Shipment scheduled for order: {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("Delivery address lookup for customer: {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.info("Shipment label created for order: {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("===== Shipment Processing Complete =====");
    }
}
