package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.model.RecordContainer;
import com.demo.kafkaaggregator.services.elasticsearch.FilteredRecordService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class CreateTimeAggregatorService {
    private static String producerName = "createTimeNonSchedulerAggregator";

    private final FilteredRecordService filteredRecordService;

    @Value("${grace.period.ms}")
    private long gracePeriod;

    @Value("${window.min}")
    private long window;

    @Value("${window.threshold}")
    private long threshold;

    private RecordsCounts recordsCounts;

    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.create-time.service.group-id}")
    public synchronized void listen(ConsumerRecord<String, String> record) {
        recordsCounts.put(record.value(), record.timestamp());
    }

    @PostConstruct
    public void init() {
        log.info("grace period: " + gracePeriod);
        recordsCounts = new RecordsCounts(window * 60000, gracePeriod, threshold);
    }

    @Data
    private class RecordsCounts {
        private RecordsCounts previous;
        private RecordContainer recordContainer;
        private boolean isFirstRound = true;
        private final long windowLength;
        private final long gracePeriod;
        private long startTimestamp;
        private long lastTimestamp;

        public RecordsCounts(long windowLength, long gracePeriod, long threshold) {
            this.windowLength = windowLength;
            this.gracePeriod = gracePeriod;
            recordContainer = new RecordContainer(threshold);
        }

        public void put(String value, long timestamp) {
            if (isFirstRound) {
                handleFirstRound(value, timestamp);
            } else {
                handleNonFirstRounds(value, timestamp);
            }
        }

        private void handleFirstRound(String value, long timestamp) {
            if (isFirstRecord()) {
                startTimestamp = timestamp;
                lastTimestamp = timestamp;
                put(value);
            } else {
                handleNonFirstRecordsInFirstRound(value, timestamp);
            }
        }

        private void handleNonFirstRounds(String value, long timestamp) {
            if (belongsToPreviousRounds(timestamp)) {
                handePreviousRound(value);
            } else {
                putForward(value, timestamp);
            }
        }

        private boolean isFirstRecord() {
            return startTimestamp == 0L;
        }

        private void handleNonFirstRecordsInFirstRound(String value, long timestamp) {
            if (timestamp < startTimestamp) {
                if (lastTimestamp - timestamp < windowLength) {
                    startTimestamp = timestamp;
                    put(value);
                }
            } else
                putForward(value, timestamp);
        }

        private boolean belongsToPreviousRounds(long timestamp) {
            return timestamp < startTimestamp;
        }

        private void handePreviousRound(String value) {
            if (!gracePeriodPassed())
                previous.put(value);
        }

        private void putForward(String value, long timestamp) {
            if (belongsToThisRound(timestamp)) {
                handleThisRound(value, timestamp);
            } else {
                startNewRound(value, timestamp);
            }
        }

        private boolean belongsToThisRound(long timestamp) {
            return timestamp - startTimestamp < windowLength;
        }

        private void handleThisRound(String value, long timestamp) {
            lastTimestamp = Math.max(lastTimestamp, timestamp);
            put(value);
            if (gracePeriodPassed())
                publishPrevious();
        }

        private boolean gracePeriodPassed() {
            return lastTimestamp - startTimestamp >= gracePeriod;
        }

        private void startNewRound(String value, long timestamp) {
            publishPrevious();
            RecordsCounts recordsCounts = new RecordsCounts(windowLength, gracePeriod, threshold);
            recordsCounts.setStartTimestamp(startTimestamp + windowLength);
            recordsCounts.setFirstRound(false);
            recordsCounts.setPrevious(this);
            CreateTimeAggregatorService.this.recordsCounts = recordsCounts;
            recordsCounts.put(value, timestamp);
        }

        private void publishPrevious() {
            Optional.ofNullable(previous)
                    .ifPresent(RecordsCounts::publish);
            this.setPrevious(null);
        }

        private void publish() {
            filteredRecordService.saveAll(recordContainer.getRecordSet(producerName));
        }

        public void put(String value) {
            recordContainer.put(value);
        }
    }
}
