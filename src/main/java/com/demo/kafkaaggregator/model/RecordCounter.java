package com.demo.kafkaaggregator.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Data
public class RecordCounter {
    private final long threshold;
    private Map<String, Long> counts = new ConcurrentHashMap<>();
    private Map<String, Long> aboveThreshold = new ConcurrentHashMap<>();

    private AtomicLong userCounts = new AtomicLong(0);
    private volatile boolean isGettingPublished = false;

    public boolean put(String value) {
        if (isGettingPublished) { // for avoiding excessive waiting
            return false;
        }
        userCounts.incrementAndGet();
        if (isGettingPublished) {
            return decrementUsersAndReturn(false);
        }
        putValue(value);
        return decrementUsersAndReturn(true);
    }

    private boolean decrementUsersAndReturn(boolean x) {
        userCounts.decrementAndGet();
        return x;
    }

    private void putValue(String value) {
        long valueCount = counts.getOrDefault(value, 0L) + 1;
        counts.put(value, valueCount);
        if (valueCount >= threshold)
            aboveThreshold.put(value, valueCount);
    }

    public void blockInputs() {
        setGettingPublished(true);
    }

    public Set<FilteredRecord> getRecordSet(String producer) {
        Date date = new Date();
        return getAboveThreshold().entrySet().stream()
                .map(e -> FilteredRecord.builder().count(e.getValue()).date(date).producer(producer).url(e.getKey()).build())
                .collect(Collectors.toSet());
    }

    public boolean isFree() {
        return userCounts.get() == 0;
    }
}