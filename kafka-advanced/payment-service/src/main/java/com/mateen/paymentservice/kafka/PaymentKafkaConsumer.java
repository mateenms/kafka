package com.mateen.paymentservice.kafka;

import com.basedomain.dto.OrderEventDTO;
import com.mateen.paymentservice.entity.PaymentRecord;
import com.mateen.paymentservice.repository.PaymentRecordRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class PaymentKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentKafkaConsumer.class);

    private final PaymentRecordRepository paymentRecordRepository;

    // RETRY + DLT CONFIGURATION
    // attempts = "4" means: 1 original attempt + 3 retries
    // backoff: waits 1s, 2s, 4s between retries (doubles each time)
    // After all retries fail → message goes to order-events-dlt topic automatically
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = "-dlt"
    )
    @KafkaListener(topics = "${spring.kafka.topic.order-events}", groupId = "payment-service-group")
    public void consume(OrderEventDTO orderEventDTO,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {

        LOGGER.info("===== Payment Service: Order Event Received =====");
        LOGGER.info("Partition : {} | Offset: {}", partition, offset);
        LOGGER.info("Event ID  : {}", orderEventDTO.getEventId());
        LOGGER.info("Event Type: {}", orderEventDTO.getEventType());
        LOGGER.info("Order ID  : {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("Customer  : {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.info("Product   : {}", orderEventDTO.getOrder().getProductName());
        LOGGER.info("Amount    : {}", orderEventDTO.getOrder().getPrice());
        LOGGER.info("------ Processing Payment ------");

        // Simulate payment processing
        // In real-world: call payment gateway (Stripe, PayPal etc.)
        processPayment(orderEventDTO);

        PaymentRecord record = PaymentRecord.builder()
                .orderId(orderEventDTO.getOrder().getOrderId())
                .customerId(orderEventDTO.getOrder().getCustomerId())
                .productName(orderEventDTO.getOrder().getProductName())
                .amount(orderEventDTO.getOrder().getPrice())
                .paymentStatus("SUCCESS")
                .eventType(orderEventDTO.getEventType().name())
                .processedAt(LocalDateTime.now())
                .build();

        paymentRecordRepository.save(record);

        LOGGER.info("Payment processed and saved — Order: {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.info("===== Payment Processing Complete =====");
    }

    // DLT HANDLER — called when ALL retries are exhausted
    // The failed message lands here after 3 retries failed
    @DltHandler
    public void handleDlt(OrderEventDTO orderEventDTO,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {

        LOGGER.error("===== DLT: Message failed after all retries =====");
        LOGGER.error("DLT Topic : {}", topic);
        LOGGER.error("Partition : {} | Offset: {}", partition, offset);
        LOGGER.error("Order ID  : {}", orderEventDTO.getOrder().getOrderId());
        LOGGER.error("Customer  : {}", orderEventDTO.getOrder().getCustomerId());
        LOGGER.error("Product   : {}", orderEventDTO.getOrder().getProductName());
        LOGGER.error("Amount    : {}", orderEventDTO.getOrder().getPrice());
        LOGGER.error("Action    : Manual intervention required for this order");
        LOGGER.error("===== DLT Handler Complete =====");

        // In real-world: send alert email, create support ticket, notify ops team
    }

    private void processPayment(OrderEventDTO orderEventDTO) {
        // Simulate occasional payment failure to demonstrate retry
        // Remove this block in production
        if (orderEventDTO.getOrder().getPrice() != null &&
                orderEventDTO.getOrder().getPrice().intValue() == 1) {
            LOGGER.warn("Payment failed for order: {} — simulating failure for retry demo",
                    orderEventDTO.getOrder().getOrderId());
            throw new RuntimeException("Payment gateway timeout — will retry");
        }
        LOGGER.info("Payment gateway approved — Amount: {}", orderEventDTO.getOrder().getPrice());
    }
}
