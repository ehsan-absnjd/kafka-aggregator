package com.demo.kafkaaggregator.controllers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;

@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
public class UrlGeneratorController {
    @Value(value = "${spring.kafka.topic-name}")
    private String topicName;

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final Random random = new Random(42);

    @GetMapping("/{interval}/{count}")
    public String generateUrls(@PathVariable("interval") long interval, @PathVariable("count") long count) {
        Flux.interval(Duration.ofMillis(interval))
                .map(i -> random.nextInt(100) + ".com")
                .take(count)
                .doOnNext(i -> kafkaTemplate.send(topicName, i))
                .subscribe();
        return "url generator started with interval: " + interval + " ms and total count: " + count;
    }
}
