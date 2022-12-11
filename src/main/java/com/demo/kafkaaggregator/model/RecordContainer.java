package com.demo.kafkaaggregator.model;

import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class RecordContainer {
    private final long threshold;
    private Map<String, Long> counts = new HashMap<>();
    private Map<String, Long> finalResults = new HashMap<>();

    public void put(String value) {
        long valueCount = counts.getOrDefault(value, 0L) + 1;
        counts.put(value, valueCount);
        if (valueCount >= threshold)
            finalResults.put(value, valueCount);
    }

    public Map<String, Long> getResults() {
        return finalResults;
    }
}