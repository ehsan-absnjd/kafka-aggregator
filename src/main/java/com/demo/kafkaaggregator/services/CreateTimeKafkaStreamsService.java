package com.demo.kafkaaggregator.services;

import com.demo.kafkaaggregator.model.FilteredRecord;
import com.demo.kafkaaggregator.services.elasticsearch.FilteredRecordService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.time.Duration;
import java.util.Set;

@Service
@Slf4j
@RequiredArgsConstructor
public class CreateTimeKafkaStreamsService {
    private static String producerName = "kafkaStreams";

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private final FilteredRecordService service;

    @Value("${window.threshold}")
    private long threshold;

    @Value("${window.min}")
    private long windowMins;

    @Value("${grace.period.ms}")
    private long gracePeriodMs;

    @Value("${spring.kafka.topic-name}")
    private String inputTopic;

    @Value("${kafka.streams.results.topic}")
    private String resultTopicName;

    @Value("${kafka.streams.state.store}")
    private String kafkaStateStoreName;

    @KafkaListener(topics = "${kafka.streams.results.topic}", groupId = "${spring.kafka.streams.service.consumer.group-id}",
            properties = {"value.deserializer:com.demo.kafkaaggregator.configs.CustomJsonDeserializer",
            "max.poll.records:${spring.kafka.streams.service.consumer.batch}"})
    public void listen(@Payload Set<FilteredRecord> records) {
        service.saveAll(records);
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {

        KStream<String, String> messageStream = streamsBuilder
                .stream(inputTopic, Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, FilteredRecord> urlCounts = messageStream
                .groupBy((k, v) -> v, Grouped.with(STRING_SERDE, STRING_SERDE))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(windowMins)).grace(Duration.ofMillis(gracePeriodMs)))
                .count(Materialized.as(kafkaStateStoreName))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .filter((k, v) -> v > threshold)
                .map((stringWindowed, aLong) -> KeyValue.pair(stringWindowed.key(), FilteredRecord.builder()
                        .date(Date.from(stringWindowed.window().endTime())).url(stringWindowed.key()).count(aLong)
                        .producer(producerName).build()));
        urlCounts.to(resultTopicName, Produced.with(STRING_SERDE, new JsonSerde()));
    }
}
