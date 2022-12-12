package com.demo.kafkaaggregator.model;

import lombok.RequiredArgsConstructor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class RecordContainer {
    private final long threshold;
    private Map<String, Long> counts = new HashMap<>();

    public void put(String value) {
        long valueCount = counts.getOrDefault(value, 0L) + 1;
        counts.put(value, valueCount);
    }

    public Map<String, Long> getResults() {
        return counts.entrySet().stream().filter(e -> e.getValue() >= threshold)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Set<FilteredRecord> getRecordSet(String producer) {
        Date date = new Date();
        return getResults().entrySet().stream()
                .map(e -> FilteredRecord.builder().count(e.getValue()).date(date).producer(producer).url(e.getKey()).build())
                .collect(Collectors.toSet());
    }
}