package com.ms.wikimedia_data_producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declares the Kafka topic; KafkaAdmin creates it on startup if absent. */
@Configuration
public class KafkaTopicConfig {

	/** Creates the wikimedia recent-changes topic with 1 partition and 1 replica. */
	@Bean
	public NewTopic wikimediaRecentChangesTopic(@Value("${wikimedia.kafka.topic}") String topicName) {
		return TopicBuilder.name(topicName)
				.partitions(1)
				.replicas(1)
				.build();
	}
}
