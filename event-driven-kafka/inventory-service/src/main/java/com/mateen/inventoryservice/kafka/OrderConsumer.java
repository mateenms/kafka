package com.mateen.inventoryservice.kafka;

import com.basedomain.dto.OrderEventDTO;
import com.mateen.inventoryservice.entity.OrderInventory;
import com.mateen.inventoryservice.repository.OrderInventoryRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class OrderConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderInventoryRepository orderInventoryRepository;

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "inventory-service-group")
    public void consume(OrderEventDTO orderEventDTO) {
        LOGGER.info("===== Inventory Service: New Kafka Event Received =====");
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
        LOGGER.info("------ Processing Inventory ------");

        OrderInventory inventory = OrderInventory.builder()
                .orderId(orderEventDTO.getOrder().getOrderId())
                .customerId(orderEventDTO.getOrder().getCustomerId())
                .productId(orderEventDTO.getOrder().getProductId())
                .productName(orderEventDTO.getOrder().getProductName())
                .quantity(orderEventDTO.getOrder().getQuantity())
                .price(orderEventDTO.getOrder().getPrice())
                .eventType(orderEventDTO.getEventType().name())
                .status(orderEventDTO.getOrder().getStatus().name())
                .createdAt(LocalDateTime.now())
                .build();

        orderInventoryRepository.save(inventory);
        LOGGER.info("Inventory record saved to DB for order: {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("===== Inventory Processing Complete =====");
    }
}
