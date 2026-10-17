package com.ms.wikimedia_data_producer.stream;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/** Opens the Wikimedia SSE stream and forwards each event to Kafka. */
@Component
public class WikimediaChangesProducer {

	private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class);

	/** How long (in minutes) to keep the Wikimedia stream open before the app exits. */
	private static final int STREAM_DURATION_MINUTES = 1;

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final String topic;
	private final String streamUrl;
	private final String userAgent;

	private BackgroundEventSource eventSource;

	/** Creates the producer with all dependencies injected from application config. */
	public WikimediaChangesProducer(
			KafkaTemplate<String, String> kafkaTemplate,
			@Value("${wikimedia.kafka.topic}") String topic,
			@Value("${wikimedia.stream.url}") String streamUrl,
			@Value("${wikimedia.stream.user-agent}") String userAgent) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
		this.streamUrl = streamUrl;
		this.userAgent = userAgent;
	}

	/** Connects to the stream and blocks for STREAM_DURATION_MINUTES; idempotent. */
	public synchronized void startStream() throws InterruptedException {
		if (eventSource != null) {
			return;
		}

		WikimediaChangesHandler handler = new WikimediaChangesHandler(kafkaTemplate, topic);

		EventSource.Builder builder = new EventSource.Builder(
				ConnectStrategy.http(URI.create(streamUrl))
						.header("User-Agent", userAgent));

		eventSource = new BackgroundEventSource.Builder(handler, builder).build();
		eventSource.start();

		log.info("Wikimedia SSE stream started — url={}, topic={}", streamUrl, topic);
		TimeUnit.MINUTES.sleep(STREAM_DURATION_MINUTES);
	}
}
