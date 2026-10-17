package com.basedomain.dto;

import com.basedomain.enums.OrderEventType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEventDTO {
    private String eventId;
    private OrderEventType eventType;
    private OrderDTO order;
    private String message;
    private String status;
    private LocalDateTime createdAt;
}
