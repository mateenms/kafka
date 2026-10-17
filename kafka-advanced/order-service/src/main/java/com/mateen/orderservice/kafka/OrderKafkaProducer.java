package com.mateen.orderservice.kafka;

import com.basedomain.dto.OrderEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderKafkaProducer.class);

    @Value("${spring.kafka.topic.order-events}")
    private String orderEventsTopic;

    private final KafkaTemplate<String, OrderEventDTO> kafkaTemplate;

    public OrderKafkaProducer(KafkaTemplate<String, OrderEventDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(OrderEventDTO orderEventDTO) {
        LOGGER.info("Sending order event to topic: {}", orderEventsTopic);
        LOGGER.info("Partition key (customerId): {}", orderEventDTO.getOrder().getCustomerId());

        // KEY = customerId — ensures all orders for the same customer go to the SAME partition
        // This guarantees message ORDER per customer (e.g. place → cancel → refund in correct order)
        CompletableFuture<SendResult<String, OrderEventDTO>> future =
                kafkaTemplate.send(orderEventsTopic, orderEventDTO.getOrder().getCustomerId(), orderEventDTO);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                LOGGER.info("Order event sent successfully");
                LOGGER.info("Topic    : {}", result.getRecordMetadata().topic());
                // Which partition this message landed on (based on customerId hash)
                LOGGER.info("Partition: {}", result.getRecordMetadata().partition());
                LOGGER.info("Offset   : {}", result.getRecordMetadata().offset());
            } else {
                LOGGER.error("Failed to send order event for orderId: {} — {}",
                        orderEventDTO.getOrder().getOrderId(), ex.getMessage());
            }
        });
    }
}
