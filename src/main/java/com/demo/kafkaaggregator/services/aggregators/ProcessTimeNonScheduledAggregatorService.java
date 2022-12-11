package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.model.RecordContainer;
import com.demo.kafkaaggregator.services.elasticsearch.FilteredRecordService;
import com.demo.kafkaaggregator.services.time.TimeService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

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

    private RecordContainer recordContainer;

    private long startTime;

    private long currentRound;

    @Value("${window.min}")
    private long window;

    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.non-scheduler.service.group-id}")
    public synchronized void listen(ConsumerRecord<String, String> record) {
        init();
        recordContainer.put(record.value());
    }

    private void init() {
        if (isStart()) {
            startTime = timeService.getCurrentMillis();
            recordContainer = new RecordContainer(threshold);
        } else {
            publishResultsAndStartNewRoundIfNecessary();
        }
    }

    private void publishResultsAndStartNewRoundIfNecessary() {
        long calculatedRound = (timeService.getCurrentMillis() - startTime) / (window * 60000);
        if (calculatedRound > currentRound) {
            filteredRecordService.saveAll(recordContainer.getRecordSet(producerName));
            currentRound = calculatedRound;
            recordContainer = new RecordContainer(threshold);
        }
    }

    private boolean isStart() {
        return startTime == 0l;
    }
}
