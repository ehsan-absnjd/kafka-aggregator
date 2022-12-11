package com.demo.kafkaaggregator.services;

import com.demo.kafkaaggregator.model.RecordContainer;
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

    private RecordContainer container;

    @Scheduled(fixedRateString = "${scheduler.min}")
    public synchronized void refresh() {
        log.info("scheduler starting.");
        Optional.ofNullable(container).map(c-> c.getRecordSet(producerName))
                .ifPresent(service::saveAll);
        container = new RecordContainer(threshold);
    }

    @KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.scheduler.service.group-id}")
    public synchronized void listen(ConsumerRecord<String, String> record) {
        container.put(record.value());
    }
}
