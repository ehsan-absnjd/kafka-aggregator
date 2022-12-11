package com.demo.kafkaaggregator.services.time;

import org.springframework.stereotype.Service;

@Service
public class TimeService {

    public long getCurrentMillis() {
        return System.currentTimeMillis();
    }
}
