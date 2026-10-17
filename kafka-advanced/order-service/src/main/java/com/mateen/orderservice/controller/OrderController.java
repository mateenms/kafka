package com.mateen.orderservice.controller;

import com.basedomain.dto.OrderDTO;
import com.basedomain.dto.OrderEventDTO;
import com.basedomain.enums.OrderEventType;
import com.basedomain.enums.OrderStatus;
import com.mateen.orderservice.kafka.OrderKafkaProducer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderController.class);

    private final OrderKafkaProducer orderKafkaProducer;

    @PostMapping
    public ResponseEntity<String> placeOrder(@RequestBody OrderDTO orderDTO) {
        LOGGER.info("Received order request for customer: {}", orderDTO.getCustomerId());

        orderDTO.setOrderId(UUID.randomUUID().toString());
        orderDTO.setStatus(OrderStatus.PENDING);
        orderDTO.setCreatedAt(LocalDateTime.now());

        OrderEventDTO orderEventDTO = OrderEventDTO.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(OrderEventType.ORDER_PLACED)
                .order(orderDTO)
                .message("Order placed successfully")
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();

        orderKafkaProducer.sendMessage(orderEventDTO);

        LOGGER.info("Order placed — ID: {} | Customer: {} | Product: {}",
                orderDTO.getOrderId(), orderDTO.getCustomerId(), orderDTO.getProductName());

        return ResponseEntity.ok("Order placed successfully — ID: " + orderDTO.getOrderId());
    }
}
