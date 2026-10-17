package com.ms.wikimedia_data_producer.stream;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Handles SSE lifecycle callbacks from the Wikimedia stream and publishes each
 * event payload to Kafka. Not a Spring bean — instantiated directly by
 * {@link WikimediaChangesProducer}.
 */
public class WikimediaChangesHandler implements BackgroundEventHandler {

	private static final Logger log = LoggerFactory.getLogger(WikimediaChangesHandler.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final String topic;

	/** Creates the handler with the Kafka client and destination topic. */
	public WikimediaChangesHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}

	/** Logs that the stream connection opened. */
	@Override
	public void onOpen() {
		log.info("Connected to the Wikimedia changes stream");
	}

	/** Logs that the stream connection closed. */
	@Override
	public void onClosed() {
		log.info("Disconnected from the Wikimedia changes stream");
	}

	/** Logs SSE heartbeat comments sent by Wikimedia (carry no event data). */
	@Override
	public void onComment(String comment) {
		log.debug("Received Wikimedia stream comment: {}", comment);
	}

	/** Publishes the JSON payload of each received SSE event to Kafka. */
	@Override
	public void onMessage(String event, MessageEvent messageEvent) {
		log.debug("Received Wikimedia stream event: {}", event);
		kafkaTemplate.send(topic, messageEvent.getData())
				.whenComplete((result, error) -> {
					if (error != null) {
						log.error("Failed to publish event to Kafka topic '{}'", topic, error);
					} else {
						log.debug("Published event to Kafka topic '{}'", topic);
					}
				});
	}

	/** Logs stream errors; the SSE client retries automatically. */
	@Override
	public void onError(Throwable error) {
		log.error("Error in the Wikimedia changes stream", error);
	}
}
