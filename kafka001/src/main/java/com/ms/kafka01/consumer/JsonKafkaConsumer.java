package com.ms.kafka01.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ms.kafka01.payload.User;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(JsonKafkaConsumer.class);
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.json-topic}")
    private String topic;

    @Value("${spring.kafka.consumer.json-group-id}")
    private String groupId;

    public JsonKafkaConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${spring.kafka.json-topic}",
            groupId = "${spring.kafka.consumer.json-group-id}")
    public void consume(String json) {
        try {
            User user = objectMapper.readValue(json, User.class);
            logger.info("Received JSON user from topic {}: {}", topic, user);
        } catch (JsonProcessingException exception) {
            logger.error("Could not deserialize JSON user from topic {}: {}", topic, json, exception);
        }
    }
}
