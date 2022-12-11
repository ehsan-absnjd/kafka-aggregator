package com.demo.kafkaaggregator.model;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class RecordContainerTest {

    @Test
    void givenARecordContainerWithThreshold_whenThereAreNotAnyRecordsWithEnoughCount_shouldReturnEmpty() {
        RecordContainer recordContainer = new RecordContainer(2);
        recordContainer.put("value");
        recordContainer.put("anotherValue");

        assertThat(recordContainer.getResults()).hasSize(0);
    }

    @Test
    void givenARecordContainerWithThreshold_whenThereAreOnlyOneRecordWithEnoughCount_shouldReturnCorrectResults() {
        RecordContainer recordContainer = new RecordContainer(2);
        recordContainer.put("value");
        recordContainer.put("value");
        recordContainer.put("anotherValue");

        Set<Map.Entry<String, Long>> actual = recordContainer.getResults().entrySet();
        assertThat(actual).hasSize(1);
        assertThat(actual).containsExactly(Map.entry("value", 2L));
    }

    @Test
    void givenARecordContainerWithThreshold_whenThereAreOnlyTwoRecordsWithEnoughCount_shouldReturnCorrectResults() {
        RecordContainer recordContainer = new RecordContainer(2);
        recordContainer.put("value");
        recordContainer.put("value");
        recordContainer.put("anotherValue");
        recordContainer.put("anotherValue");

        Set<Map.Entry<String, Long>> actual = recordContainer.getResults().entrySet();
        assertThat(actual).hasSize(2);
        assertThat(actual).containsExactlyInAnyOrder(Map.entry("value", 2L), Map.entry("anotherValue", 2L));
    }

}