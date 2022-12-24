package com.demo.kafkaaggregator.services.aggregators;

import com.demo.kafkaaggregator.model.RecordCounter;
import com.demo.kafkaaggregator.services.elasticsearch.FilteredRecordService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Data
@Slf4j
@RequiredArgsConstructor
public class ProcessTimeScheduledAggregatorService {
    private static String producerName = "processTimeSchedulerAggregator";
    private final FilteredRecordService service;

    @Value("${window.threshold}")
    private long threshold;

    private volatile RecordCounter counter;

    @Scheduled(fixedRateString = "${scheduler.min}")
    public void refresh() {
        log.info("scheduler starting.");
        Optional.ofNullable(counter).ifPresent(this::tryPublishing);
        counter = new RecordCounter(threshold);
    }

    private void tryPublishing(RecordCounter records) {
        records.blockInputs();
        while (!counter.isFree()) {
        }
        service.saveAll(counter.getRecordSet(producerName));
    }

    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.scheduler.service.group-id}")
    public void listen(ConsumerRecord<String, String> record) {
        counter.put(record.value());
    }
}
