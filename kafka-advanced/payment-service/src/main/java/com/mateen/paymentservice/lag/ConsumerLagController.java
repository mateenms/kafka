package com.mateen.paymentservice.lag;

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

    /**
     * GET /api/consumer-lag
     *
     * Returns how far behind this service's consumer group is from the latest Kafka offsets.
     *
     * Response:
     * {
     *   "groupId": "payment-service-group",
     *   "status": "HEALTHY" | "LAGGING" | "NO_COMMITTED_OFFSETS" | "ERROR",
     *   "totalLag": 0,
     *   "partitions": [
     *     {
     *       "topic": "order-events",
     *       "partition": 0,
     *       "currentOffset": 42,
     *       "endOffset": 42,
     *       "lag": 0,
     *       "status": "CAUGHT_UP"
     *     }
     *   ]
     * }
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getConsumerLag() {
        return ResponseEntity.ok(consumerLagService.getConsumerLag());
    }
}
