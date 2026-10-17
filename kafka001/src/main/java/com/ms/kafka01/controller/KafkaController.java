package com.ms.kafka01.controller;

import com.ms.kafka01.payload.User;
import com.ms.kafka01.producer.JsonKafkaProducer;
import com.ms.kafka01.producer.KafkaProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaProducer kafkaProducer;



    private final JsonKafkaProducer jsonKafkaProducer;

    public KafkaController(KafkaProducer kafkaProducer, JsonKafkaProducer jsonKafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.jsonKafkaProducer = jsonKafkaProducer;
    }

    @PostMapping("/message")
    public ResponseEntity<String> sendMessage(@RequestBody MessageRequest request) {
        kafkaProducer.sendMessage(request.message());
        return ResponseEntity.accepted().body("Message accepted for sending to Kafka");
    }


    @PostMapping("/json-message")
    public ResponseEntity<String> sendJsonMessage(@RequestBody User user) {
        jsonKafkaProducer.sendUser(user);
        return ResponseEntity.accepted().body("Json Message sent to Kafka Topic.");
    }


    public record MessageRequest(String message) {
    }
}
