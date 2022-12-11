package com.demo.kafkaaggregator.controllers;

import com.demo.kafkaaggregator.model.FilteredRecord;
import com.demo.kafkaaggregator.services.elasticsearch.FilteredRecordService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class FilteredRecordController {
    private final FilteredRecordService service;

    @GetMapping("/data")
    public Iterable<FilteredRecord> getAll(){
        return service.getAll();
    }
}
