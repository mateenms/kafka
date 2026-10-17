package com.ms.wikimedia_data_producer;

import com.ms.wikimedia_data_producer.stream.WikimediaChangesProducer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Starts the Wikimedia Data Producer application. */
@SpringBootApplication
public class WikimediaDataProducerApplication implements CommandLineRunner {

	private final WikimediaChangesProducer wikiMediaChangesProducer;

	/** Injects the stream producer. */
	public WikimediaDataProducerApplication(WikimediaChangesProducer wikiMediaChangesProducer) {
		this.wikiMediaChangesProducer = wikiMediaChangesProducer;
	}

	/** Application entry point. */
	public static void main(String[] args) {
		SpringApplication.run(WikimediaDataProducerApplication.class, args);
	}

	/** Opens the Wikimedia SSE stream after the context starts. */
	@Override
	public void run(String... args) throws InterruptedException {
		wikiMediaChangesProducer.startStream();
	}
}
