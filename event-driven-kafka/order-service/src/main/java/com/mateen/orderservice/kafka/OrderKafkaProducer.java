package com.mateen.orderservice.kafka;


import com.basedomain.dto.OrderEventDTO;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class OrderKafkaProducer {


    private static final Logger LOGGER = LoggerFactory.getLogger(OrderKafkaProducer.class);

    private NewTopic orderTopic;

    private KafkaTemplate<String, OrderEventDTO> kafkaTemplate;


    public OrderKafkaProducer(KafkaTemplate<String, OrderEventDTO> kafkaTemplate, NewTopic orderTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderTopic = orderTopic;
    }


    public void sendMessage(OrderEventDTO orderEventDTO){
        LOGGER.info("Order event received in OrderKafkaProducer: {}", orderEventDTO);

        Message<OrderEventDTO> message = MessageBuilder
                .withPayload(orderEventDTO)
                .setHeader(KafkaHeaders.TOPIC, orderTopic.name())
                .build();

        kafkaTemplate.send(message);

        LOGGER.info("Order event sent to topic '{}' successfully", orderTopic.name());
    }
}
