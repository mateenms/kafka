package com.ms.wikimedia_data_consumer.consumer;

import com.ms.wikimedia_data_consumer.entity.WikiEventData;
import com.ms.wikimedia_data_consumer.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Listens to the Wikimedia Kafka topic and persists each event to PostgreSQL. */
@Component
public class WikimediaChangesConsumer {

	private static final Logger log = LoggerFactory.getLogger(WikimediaChangesConsumer.class);

	private final WikimediaDataRepository repository;

	/** Injects the repository used to save events. */
	public WikimediaChangesConsumer(WikimediaDataRepository repository) {
		this.repository = repository;
	}

	/** Receives a Wikimedia change event from Kafka and saves it to the database. */
	@KafkaListener(
			topics = "${wikimedia.kafka.topic}",
			groupId = "${spring.kafka.consumer.group-id}"
	)
	public void consume(String message) {
		log.info("Received Wikimedia change event — saving to DB");
		WikiEventData wikiEventData = new WikiEventData();
		wikiEventData.setEventData(message);
		repository.save(wikiEventData);
	}
}
