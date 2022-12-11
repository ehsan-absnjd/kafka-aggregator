package com.demo.kafkaaggregator.repositories;

import com.demo.kafkaaggregator.model.FilteredRecord;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FilteredRecordRepository extends ElasticsearchRepository<FilteredRecord, String> {
}
