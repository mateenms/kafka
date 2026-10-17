package com.ms.kafka01.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ms.kafka01.payload.User;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaProducer {


    private static final Logger logger = LoggerFactory.getLogger(JsonKafkaProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    @Value("${spring.kafka.json-topic}")
    private String topic;

    public JsonKafkaProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendUser(User user) {
        try {
            kafkaTemplate.send(topic, objectMapper.writeValueAsString(user));
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Could not serialize user as JSON", exception);
        }
        logger.info("JSON user payload sent to Kafka topic {}", topic);
    }
}
