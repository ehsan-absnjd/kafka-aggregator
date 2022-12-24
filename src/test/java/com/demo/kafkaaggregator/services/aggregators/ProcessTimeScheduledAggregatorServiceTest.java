package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.model.RecordCounter;
import com.demo.kafkaaggregator.repositories.FilteredRecordRepository;
import com.demo.kafkaaggregator.services.elasticsearch.FilteredRecordService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class ProcessTimeScheduledAggregatorServiceTest {
    @Autowired
    ProcessTimeScheduledAggregatorService service;

    @Autowired
    FilteredRecordService filteredRecordService;

    @Autowired
    FilteredRecordRepository repository;

    @BeforeEach
    public void init() {
        repository.deleteAll();
        service.setThreshold(2);
    }

    @Test
    void whenRefreshingService_itsContainerShouldBeReset() {
        RecordCounter container = new RecordCounter(2);
        service.setCounter(container);

        service.refresh();
        assertThat(service.getCounter()).isNotEqualTo(container);
    }

    @Test
    void givenThereAreRecordsInContainer_whenRefreshingService_theyShouldBeSaved() {
        RecordCounter container = new RecordCounter(2);
        container.put("value");
        container.put("value");
        service.setCounter(container);

        service.refresh();
        assertThat(service.getCounter()).isNotEqualTo(container);
        assertThat(repository.count()).isEqualTo(1);
    }
}