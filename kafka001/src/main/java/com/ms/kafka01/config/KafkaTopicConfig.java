package com.ms.kafka01.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.topic}")
    private String topicName;

    @Value("${spring.kafka.json-topic}")
    private String jsonTopicName;

    @Bean
    public NewTopic textTopic() {
        return TopicBuilder.name(topicName).build();
    }

    @Bean
    public NewTopic jsonTopic() {
        return TopicBuilder.name(jsonTopicName).build();
    }
}
