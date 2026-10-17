package com.mateen.orderservice.kafka;

import com.basedomain.dto.OrderDTO;
import com.basedomain.dto.OrderEventDTO;
import com.basedomain.enums.OrderEventType;
import com.basedomain.enums.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class TransactionalOrderService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalOrderService.class);

    @Value("${spring.kafka.topic.order-events}")
    private String orderEventsTopic;

    @Value("${spring.kafka.topic.order-confirmed-events}")
    private String orderConfirmedTopic;

    private final KafkaTemplate<String, OrderEventDTO> kafkaTemplate;

    public TransactionalOrderService(KafkaTemplate<String, OrderEventDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends TWO messages to TWO topics inside a single Kafka transaction.
     *
     * Either BOTH messages are committed, or NEITHER is visible to consumers.
     *
     * This prevents the scenario where:
     *   - order-events gets the ORDER_PLACED message ✓
     *   - order-confirmed-events write fails ✗
     *   → consumers see a half-processed order
     *
     * With transactions: if the second send fails, the first is also rolled back.
     * Consumers with isolation.level=read_committed won't see either message.
     */
    public String sendTransactionally(OrderDTO orderDTO) {
        String orderId = UUID.randomUUID().toString();
        String customerId = orderDTO.getCustomerId();

        orderDTO.setOrderId(orderId);
        orderDTO.setStatus(OrderStatus.PENDING);
        orderDTO.setCreatedAt(LocalDateTime.now());

        // Event 1 — ORDER_PLACED
        OrderEventDTO placedEvent = OrderEventDTO.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(OrderEventType.ORDER_PLACED)
                .order(orderDTO)
                .message("Order placed — transactional send")
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();

        // Event 2 — ORDER_CONFIRMED (written atomically with the first)
        OrderEventDTO confirmedEvent = OrderEventDTO.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(OrderEventType.ORDER_CONFIRMED)
                .order(orderDTO)
                .message("Order confirmed — same transaction")
                .status("CONFIRMED")
                .createdAt(LocalDateTime.now())
                .build();

        LOGGER.info("Starting Kafka transaction for orderId: {} | customerId: {}", orderId, customerId);

        // executeInTransaction — wraps both sends in one atomic transaction
        // beginTransaction() → send() → send() → commitTransaction()
        // If any send fails → abortTransaction() → consumers see nothing
        kafkaTemplate.executeInTransaction(ops -> {
            LOGGER.info("TX: Sending ORDER_PLACED to '{}' — partition key: {}", orderEventsTopic, customerId);
            ops.send(orderEventsTopic, customerId, placedEvent);

            LOGGER.info("TX: Sending ORDER_CONFIRMED to '{}' — partition key: {}", orderConfirmedTopic, customerId);
            ops.send(orderConfirmedTopic, customerId, confirmedEvent);

            LOGGER.info("TX: Both sends queued — committing transaction");
            return true;
        });

        LOGGER.info("Transaction committed — orderId: {} | both topics written atomically", orderId);
        return orderId;
    }
}
