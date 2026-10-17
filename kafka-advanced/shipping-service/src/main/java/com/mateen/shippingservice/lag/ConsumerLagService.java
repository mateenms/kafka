package com.mateen.shippingservice.lag;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class ConsumerLagService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerLagService.class);

    private final AdminClient adminClient;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    public ConsumerLagService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public Map<String, Object> getConsumerLag() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupId", groupId);

        try {
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committedOffsets =
                    offsetsResult.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);

            if (committedOffsets == null || committedOffsets.isEmpty()) {
                result.put("status", "NO_COMMITTED_OFFSETS");
                result.put("message", "Consumer group has not committed any offsets yet — service may not have consumed any messages");
                return result;
            }

            Map<TopicPartition, OffsetSpec> latestOffsetRequest = committedOffsets.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

            Map<TopicPartition, Long> endOffsets = adminClient
                    .listOffsets(latestOffsetRequest)
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            List<Map<String, Object>> partitionLags = new ArrayList<>();
            long totalLag = 0;

            for (Map.Entry<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> entry : committedOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long currentOffset = entry.getValue().offset();
                long endOffset = endOffsets.getOrDefault(tp, currentOffset);
                long lag = Math.max(0, endOffset - currentOffset);
                totalLag += lag;

                Map<String, Object> partitionInfo = new LinkedHashMap<>();
                partitionInfo.put("topic", tp.topic());
                partitionInfo.put("partition", tp.partition());
                partitionInfo.put("currentOffset", currentOffset);
                partitionInfo.put("endOffset", endOffset);
                partitionInfo.put("lag", lag);
                partitionInfo.put("status", lag == 0 ? "CAUGHT_UP" : "LAGGING");
                partitionLags.add(partitionInfo);

                LOGGER.info("Consumer lag — Group: {} | Topic: {} | Partition: {} | Lag: {}",
                        groupId, tp.topic(), tp.partition(), lag);
            }

            result.put("status", totalLag == 0 ? "HEALTHY" : "LAGGING");
            result.put("totalLag", totalLag);
            result.put("partitions", partitionLags);

        } catch (Exception e) {
            LOGGER.error("Failed to calculate consumer lag for group {}: {}", groupId, e.getMessage());
            result.put("status", "ERROR");
            result.put("error", e.getMessage());
        }

        return result;
    }
}
