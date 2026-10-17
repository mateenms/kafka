package com.ms.wikimedia_data_consumer.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

/** Stores one Wikimedia recent-change event received from Kafka. */
@Entity
@Table(name = "wikimedia_data")
@Getter
@Setter
@NoArgsConstructor
public class WikiEventData {

    /** Auto-generated primary key. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Raw JSON payload — stored as a large object to handle arbitrarily long events. */
    @Lob
    private String eventData;

    /** Timestamp when this record was persisted. */
    private LocalDateTime createdAt;

    /** Sets createdAt to now before the first persist. */
    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
    }
}
