package com.mateen.inventoryservice.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "order_inventory")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderInventory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String orderId;
    private String customerId;
    private String productId;
    private String productName;
    private Integer quantity;
    private BigDecimal price;
    private String eventType;
    private String status;
    private LocalDateTime createdAt;
}
