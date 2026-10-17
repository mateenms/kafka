package com.mateen.orderservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.topic.order-events}")
    private String orderEventsTopic;

    @Value("${spring.kafka.topic.order-confirmed-events}")
    private String orderConfirmedEventsTopic;

    // 3 partitions — messages are routed by customerId key
    // same customerId always lands on same partition = guaranteed order per customer
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder
                .name(orderEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Confirmation topic — written atomically with order-events in a Kafka transaction
    @Bean
    public NewTopic orderConfirmedEventsTopic() {
        return TopicBuilder
                .name(orderConfirmedEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
