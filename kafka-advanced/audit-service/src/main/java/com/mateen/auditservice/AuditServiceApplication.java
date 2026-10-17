package com.mateen.auditservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

// @EnableKafkaStreams — activates Kafka Streams processing in this Spring Boot app
// Without this annotation, the StreamsBuilder bean won't be processed
@SpringBootApplication
@EnableKafkaStreams
public class AuditServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuditServiceApplication.class, args);
    }
}
