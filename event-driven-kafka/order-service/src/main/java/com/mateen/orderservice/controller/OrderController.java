package com.mateen.orderservice.controller;

import com.basedomain.dto.OrderDTO;
import com.basedomain.dto.OrderEventDTO;
import com.basedomain.enums.OrderEventType;
import com.basedomain.enums.OrderStatus;
import com.mateen.orderservice.kafka.OrderKafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderKafkaProducer orderKafkaProducer;

    @PostMapping("/orders")
    public ResponseEntity<String> placeOder(@RequestBody OrderDTO orderDTO) {

        orderDTO.setOrderId(UUID.randomUUID().toString());
        orderDTO.setStatus(OrderStatus.PENDING);
        orderDTO.setCreatedAt(LocalDateTime.now());

        OrderEventDTO orderEventDTO = new OrderEventDTO();
        orderEventDTO.setEventId(UUID.randomUUID().toString());
        orderEventDTO.setEventType(OrderEventType.ORDER_PLACED);
        orderEventDTO.setOrder(orderDTO);
        orderEventDTO.setCreatedAt(LocalDateTime.now());

        orderKafkaProducer.sendMessage(orderEventDTO);
        return ResponseEntity.ok("Order placed Successfully");
    }
}
