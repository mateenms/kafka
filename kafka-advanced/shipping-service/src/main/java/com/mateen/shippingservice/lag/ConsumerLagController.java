package com.mateen.shippingservice.lag;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/consumer-lag")
public class ConsumerLagController {

    private final ConsumerLagService consumerLagService;

    public ConsumerLagController(ConsumerLagService consumerLagService) {
        this.consumerLagService = consumerLagService;
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> getConsumerLag() {
        return ResponseEntity.ok(consumerLagService.getConsumerLag());
    }
}
