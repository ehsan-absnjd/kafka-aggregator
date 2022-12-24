package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.model.RecordCounter;
import com.demo.kafkaaggregator.services.elasticsearch.FilteredRecordService;
import com.demo.kafkaaggregator.services.time.TimeService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Data
@Slf4j
@RequiredArgsConstructor
public class ProcessTimeNonScheduledAggregatorService {
    private static String producerName = "processTimeNonSchedulerAggregator";

    private final FilteredRecordService filteredRecordService;

    private final TimeService timeService;

    @Value("${window.threshold}")
    private long threshold;

    private volatile RecordCounter recordCounter;

    private AtomicBoolean isStart = new AtomicBoolean(true);

    private long startTime;

    private AtomicLong currentRound = new AtomicLong(0);

    @Value("${window.min}")
    private long window;

    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.non-scheduler.service.group-id}")
    public void listen(ConsumerRecord<String, String> record) {
        init();
        tryPuttingInRecordCounter(record.value());
    }

    private void tryPuttingInRecordCounter(String value) {
        while (!recordCounter.put(value)) {
        }
    }

    private void init() {
        if (isStart.get() && isStart.compareAndSet(true, false)) {
            startTime = timeService.getCurrentMillis();
            recordCounter = new RecordCounter(threshold);
        } else {
            publishResultsAndStartNewRoundIfNecessary();
        }
    }

    private void publishResultsAndStartNewRoundIfNecessary() {
        long calculatedRound = getCalculatedRound();
        long current = currentRound.get();
        if (calculatedRound > current && currentRound.compareAndSet(current, calculatedRound)) {
            recordCounter.blockInputs();
            tryPublishing();
            recordCounter = new RecordCounter(threshold);
        }
    }

    private long getCalculatedRound() {
        return (timeService.getCurrentMillis() - startTime) / (window * 60000);
    }

    private void tryPublishing() {
        while (!recordCounter.isFree()) {
        }
        filteredRecordService.saveAll(recordCounter.getRecordSet(producerName));
    }
}
