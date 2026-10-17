package com.mateen.shippingservice.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaErrorHandlerConfig.class);

    // GLOBAL ERROR HANDLER — catches all consumer exceptions across ALL listeners in this service
    // FixedBackOff(1000, 3) = retry every 1 second, max 3 attempts
    // After 3 attempts the message is skipped (logged here) and consumer moves to next message
    @Bean
    public DefaultErrorHandler errorHandler() {
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 3L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) -> {
            // This block runs when ALL retries are exhausted
            LOGGER.error("===== Global Error Handler: All retries exhausted =====");
            LOGGER.error("Topic    : {}", record.topic());
            LOGGER.error("Partition: {}", record.partition());
            LOGGER.error("Offset   : {}", record.offset());
            LOGGER.error("Key      : {}", record.key());
            LOGGER.error("Error    : {}", exception.getMessage());
            LOGGER.error("Action   : Message skipped — investigate manually");
            LOGGER.error("===== Error Handler Complete =====");

            // In real-world: send to alerting system, create incident ticket
        }, fixedBackOff);

        // Do NOT retry on these exceptions — they are non-recoverable
        // Retrying a deserialization error will always fail, no point retrying
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                NullPointerException.class
        );

        return errorHandler;
    }
}
