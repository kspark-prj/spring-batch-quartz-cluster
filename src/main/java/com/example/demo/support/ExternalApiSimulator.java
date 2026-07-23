package com.example.demo.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 지연 시간이 수반되는 외부 REST API 호출 시뮬레이터입니다.
 */
@Component
public class ExternalApiSimulator {

    private static final Logger log = LoggerFactory.getLogger(ExternalApiSimulator.class);

    public String callExternalValidationApi(Integer customerId, String email) {
        long startTime = System.currentTimeMillis();
        
        long mockDelay = ThreadLocalRandom.current().nextLong(300, 700);
        try {
            Thread.sleep(mockDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long duration = System.currentTimeMillis() - startTime;
        log.debug("API processed customerId: {}, delay: {}ms, thread: {}", 
                customerId, duration, Thread.currentThread());
        
        return "API_SUCCESS_CODE_200_LATENCY_" + duration + "ms";
    }
}
