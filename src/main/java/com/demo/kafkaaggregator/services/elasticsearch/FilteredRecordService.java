package com.demo.kafkaaggregator.services.elasticsearch;

import com.demo.kafkaaggregator.model.FilteredRecord;
import com.demo.kafkaaggregator.repositories.FilteredRecordRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
@RequiredArgsConstructor
public class FilteredRecordService {
    private final FilteredRecordRepository repository;

    public void saveAll(Set<FilteredRecord> recordSet) {
        repository.saveAll(recordSet);
    }

    public Iterable<FilteredRecord> getAll() {
        return repository.findAll();
    }
}
