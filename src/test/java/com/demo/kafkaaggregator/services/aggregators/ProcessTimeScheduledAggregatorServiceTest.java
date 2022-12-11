package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.model.RecordContainer;
import com.demo.kafkaaggregator.repositories.FilteredRecordRepository;
import com.demo.kafkaaggregator.services.aggregators.ProcessTimeScheduledAggregatorService;
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
        RecordContainer container = new RecordContainer(2);
        service.setContainer(container);

        service.refresh();
        assertThat(service.getContainer()).isNotEqualTo(container);
    }

    @Test
    void givenThereAreRecordsInContainer_whenRefreshingService_theyShouldBeSaved() {
        RecordContainer container = new RecordContainer(2);
        container.put("value");
        container.put("value");
        service.setContainer(container);

        service.refresh();
        assertThat(service.getContainer()).isNotEqualTo(container);
        assertThat(repository.count()).isEqualTo(1);
    }
}