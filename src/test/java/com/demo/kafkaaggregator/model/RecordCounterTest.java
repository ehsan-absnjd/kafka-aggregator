package com.demo.kafkaaggregator.model;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class RecordCounterTest {

    @Test
    void givenARecordContainerWithThreshold_whenThereAreNotAnyRecordsWithEnoughCount_shouldReturnEmpty() {
        RecordCounter recordCounter = new RecordCounter(2);
        recordCounter.put("value");
        recordCounter.put("anotherValue");

        assertThat(recordCounter.getAboveThreshold()).hasSize(0);
    }

    @Test
    void givenARecordContainerWithThreshold_whenThereAreOnlyOneRecordWithEnoughCount_shouldReturnCorrectResults() {
        RecordCounter recordCounter = new RecordCounter(2);
        recordCounter.put("value");
        recordCounter.put("value");
        recordCounter.put("anotherValue");

        Set<Map.Entry<String, Long>> actual = recordCounter.getAboveThreshold().entrySet();
        assertThat(actual).hasSize(1);
        assertThat(actual).containsExactly(Map.entry("value", 2L));
    }

    @Test
    void givenARecordContainerWithThreshold_whenThereAreOnlyTwoRecordsWithEnoughCount_shouldReturnCorrectResults() {
        RecordCounter recordCounter = new RecordCounter(2);
        recordCounter.put("value");
        recordCounter.put("value");
        recordCounter.put("anotherValue");
        recordCounter.put("anotherValue");

        Set<Map.Entry<String, Long>> actual = recordCounter.getAboveThreshold().entrySet();
        assertThat(actual).hasSize(2);
        assertThat(actual).containsExactlyInAnyOrder(Map.entry("value", 2L), Map.entry("anotherValue", 2L));
    }

}