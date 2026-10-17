package com.ms.kafka01.consumer;

import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Value("${spring.kafka.topic}")
    private String topic;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @KafkaListener(
            topics = "${spring.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String message) {
        logger.info("Received message from Kafka: {}", message);
    }
}
