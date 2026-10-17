package com.mateen.inventoryservice.repository;

import com.mateen.inventoryservice.entity.OrderInventory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderInventoryRepository extends JpaRepository<OrderInventory, Long> {
}
