package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.repositories.FilteredRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class CreateTimeAggregatorServiceTest {
    @Autowired
    CreateTimeAggregatorService service;

    @Autowired
    FilteredRecordRepository repository;

    @BeforeEach
    public void init() {
        repository.deleteAll();
        service.init();
    }

    @Test
    void whenAddingRecordWithTimestampBeforeTheStartTimestampInTheFirstRound_ifItsLowerThanLastTimestamp_startTimeStampShouldChange() {
        service.addRecord("value", 1000);
        assertThat(service.getRecordsCounts().getStartTimestamp()).isEqualTo(1000);
        long timestampWithLessThanWindowDifference = 1000 - service.getWindow() * 60000 + 1;
        service.addRecord("value", timestampWithLessThanWindowDifference);
        assertThat(service.getRecordsCounts().getStartTimestamp()).isEqualTo(timestampWithLessThanWindowDifference);
    }

    @Test
    void whenAddingRecordWithTimestampBeforeTheStartTimestampInTheFirstRound_ifItsNotLowerThanLastTimestamp_startTimeStampShouldChange() {
        service.addRecord("value", 1000);
        assertThat(service.getRecordsCounts().getStartTimestamp()).isEqualTo(1000);
        long timestampWithLessThanWindowDifference = 1000 - service.getWindow() * 60000;
        service.addRecord("value", timestampWithLessThanWindowDifference);
        assertThat(service.getRecordsCounts().getStartTimestamp()).isEqualTo(1000);
    }

    @Test
    void whenAddingRecordWithTimestampGraterThanStartTimestampPlusWindow_newRoundShouldBegin() {
        CreateTimeAggregatorService.RecordsCounts recordsCounts = service.getRecordsCounts();
        service.addRecord("value", 1000);
        assertThat(service.getRecordsCounts()).isEqualTo(recordsCounts);
        service.addRecord("value", 2000);
        assertThat(service.getRecordsCounts()).isEqualTo(recordsCounts);
        assertThat(recordsCounts.getLastTimestamp()).isEqualTo(2000);

        long windowMs = service.getWindow() * 60000;
        service.addRecord("value", 1000 + windowMs);
        CreateTimeAggregatorService.RecordsCounts newRecordCount = service.getRecordsCounts();
        assertThat(newRecordCount).isNotEqualTo(recordsCounts);
        service.addRecord("value", 2000 + windowMs);
        assertThat(service.getRecordsCounts()).isEqualTo(newRecordCount);

        service.addRecord("value", 1000 + 2 * windowMs);
        assertThat(service.getRecordsCounts()).isNotEqualTo(newRecordCount);
    }

    @Test
    void whenNewRoundHasBeganButGraceTimeHasNotPassed_nothingShouldBePublished() {
        long windowMs = service.getWindow() * 60000;
        addValueForThresholdTimes((int) service.getThreshold());
        assertThat(service.getRecordsCounts().getRecordContainer().getResults()).hasSize(1);
        assertThat(repository.count()).isEqualTo(0);

        service.addRecord("anotherValue", 1000 + windowMs + service.getGracePeriod() - 1);
        assertThat(repository.count()).isEqualTo(0);

        service.addRecord("anotherValue", 1000 + windowMs + service.getGracePeriod());
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void whenNewRoundHasBeganButGraceTimeHasNotPassed_valueShouldBeAddedToPreviousRound() {
        long windowMs = service.getWindow() * 60000;
        addValueForThresholdTimes((int) service.getThreshold() - 1);
        assertThat(service.getRecordsCounts().getRecordContainer().getResults()).hasSize(0);

        service.addRecord("anotherValue", 1000 + windowMs + service.getGracePeriod() - 1);
        service.addRecord("value", 1001);
        assertThat(service.getRecordsCounts().getPrevious().getRecordContainer().getResults()).hasSize(1);
        assertThat(repository.count()).isEqualTo(0);

        service.addRecord("anotherValue", 1000 + windowMs + service.getGracePeriod());
        assertThat(repository.count()).isEqualTo(1);
    }

    private void addValueForThresholdTimes(int times) {
        IntStream.range(0, times).forEach(i -> service.addRecord("value", 1000 + i));
    }
}