package com.ms.wikimedia_data_consumer.repository;

import com.ms.wikimedia_data_consumer.entity.WikiEventData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/** Provides CRUD operations for {@link WikiEventData} events. */
@Repository
public interface WikimediaDataRepository extends JpaRepository<WikiEventData, Long> {
}
