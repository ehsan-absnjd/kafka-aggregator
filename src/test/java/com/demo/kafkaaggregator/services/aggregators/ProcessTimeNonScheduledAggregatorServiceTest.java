package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.model.RecordCounter;
import com.demo.kafkaaggregator.repositories.FilteredRecordRepository;
import com.demo.kafkaaggregator.services.time.TimeService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

@SpringBootTest
class ProcessTimeNonScheduledAggregatorServiceTest {
    @Autowired
    ProcessTimeNonScheduledAggregatorService service;

    @Autowired
    FilteredRecordRepository repository;

    @SpyBean
    TimeService timeService;

    @BeforeEach
    public void init() {
        repository.deleteAll();
        service.setStartTime(0);
        service.getCurrentRound().set(0);
        service.getIsStart().set(true);
        service.setThreshold(2);
        service.setWindow(1);
    }

    @Test
    void whenReceivingRecordAfterWindowTime_newRecordContainerShouldBeSet() {
        mockTime(1);
        service.listen(getRecord("value"));
        RecordCounter recordCounter = service.getRecordCounter();

        mockTime(60001);
        service.listen(getRecord("value"));
        assertThat(service.getRecordCounter()).isNotEqualTo(recordCounter);
    }

    @Test
    void whenReceivingRecordAfterWindowTime_oldRecordContainersDataShouldBeSaved() {
        mockTime(1);
        service.listen(getRecord("value"));
        mockTime(10001);
        service.listen(getRecord("value"));
        mockTime(20001);
        service.listen(getRecord("otherValue"));
        RecordCounter recordCounter = service.getRecordCounter();

        mockTime(60001);
        service.listen(getRecord("value"));
        assertThat(repository.count()).isEqualTo(1);
        assertThat(service.getRecordCounter()).isNotEqualTo(recordCounter);
    }

    private void mockTime(long millis) {
        doReturn(millis).when(timeService).getCurrentMillis();
    }

    private ConsumerRecord<String, String> getRecord(String value) {
        return new ConsumerRecord<>("", 0, 0, null, value);
    }
}