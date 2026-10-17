package com.mateen.auditservice.streams;

import com.basedomain.dto.OrderEventDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;

@Configuration
public class OrderAuditStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderAuditStream.class);

    @Value("${spring.kafka.topic.order-events}")
    private String orderEventsTopic;

    @Value("${spring.kafka.topic.order-count-per-customer}")
    private String orderCountPerCustomerTopic;

    @Value("${spring.kafka.topic.revenue-per-product}")
    private String revenuePerProductTopic;

    // StreamsBuilder is injected automatically by @EnableKafkaStreams
    @Bean
    public KStream<String, String> orderAuditStream(StreamsBuilder streamsBuilder) {

        Serde<String> stringSerde = Serdes.String();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // ── Step 1: Read raw JSON messages from order-events topic ──
        // KStream = unbounded, continuously flowing stream of records
        // Key = customerId (set by order-service), Value = OrderEventDTO JSON
        KStream<String, String> orderStream = streamsBuilder.stream(
                orderEventsTopic,
                Consumed.with(stringSerde, stringSerde)
        );

        LOGGER.info("Kafka Streams: listening to topic '{}'", orderEventsTopic);

        // ── Stream 1: Count orders per customer ──
        // groupByKey   = group by existing key (customerId)
        // count()      = count how many messages per key → KTable (like a live database)
        // KTable       = stateful, always holds the LATEST value per key
        // mapValues    = convert Long count to String for output
        KTable<String, Long> orderCountTable = orderStream
                .peek((key, value) -> LOGGER.info("Stream received — customerId: {} | payload: {}", key, value))
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .count(Materialized.as("order-count-store")); // state store name for querying

        // Write count results to output topic
        // key = customerId, value = total order count
        orderCountTable
                .toStream()
                .peek((customerId, count) ->
                        LOGGER.info("Order count update — Customer: {} | Total orders: {}", customerId, count))
                .mapValues(count -> "Total orders: " + count)
                .to(orderCountPerCustomerTopic, Produced.with(stringSerde, stringSerde));

        // ── Stream 2: Total revenue per product ──
        // Re-key stream by productName so we can group by product
        // aggregate()  = custom accumulator (here: sum of prices)
        orderStream
                .mapValues(json -> {
                    try {
                        OrderEventDTO event = objectMapper.readValue(json, OrderEventDTO.class);
                        // Emit "productName:price" string for aggregation downstream
                        return event.getOrder().getProductName() + ":" + event.getOrder().getPrice();
                    } catch (Exception e) {
                        LOGGER.error("Failed to parse OrderEventDTO: {}", e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null)
                // Re-key by productName so aggregation is per product
                .selectKey((customerId, productPrice) -> productPrice.split(":")[0])
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .aggregate(
                        () -> "0",  // initializer — start revenue at 0
                        (productName, productPrice, currentRevenue) -> {
                            // Accumulate total revenue per product
                            try {
                                BigDecimal price = new BigDecimal(productPrice.split(":")[1]);
                                BigDecimal current = new BigDecimal(currentRevenue);
                                BigDecimal newRevenue = current.add(price);
                                LOGGER.info("Revenue update — Product: {} | +{} | Total: {}",
                                        productName, price, newRevenue);
                                return newRevenue.toString();
                            } catch (Exception e) {
                                LOGGER.error("Revenue aggregation error: {}", e.getMessage());
                                return currentRevenue;
                            }
                        },
                        Materialized.as("revenue-store") // state store name for querying
                )
                .toStream()
                .peek((product, revenue) ->
                        LOGGER.info("Revenue snapshot — Product: {} | Total revenue: {}", product, revenue))
                .mapValues(revenue -> "Total revenue: " + revenue)
                .to(revenuePerProductTopic, Produced.with(stringSerde, stringSerde));

        return orderStream;
    }
}
