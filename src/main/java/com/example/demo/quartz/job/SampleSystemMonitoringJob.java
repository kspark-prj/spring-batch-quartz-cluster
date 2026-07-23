package com.example.demo.quartz.job;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * 시스템 힙 메모리 모니터링을 모사하는 Quartz Job 구현체입니다.
 */
@Component
@DisallowConcurrentExecution
public class SampleSystemMonitoringJob implements org.quartz.Job {

    private static final Logger log = LoggerFactory.getLogger(SampleSystemMonitoringJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long usedHeap = memoryMXBean.getHeapMemoryUsage().getUsed() / (1024 * 1024);
        long maxHeap = memoryMXBean.getHeapMemoryUsage().getMax() / (1024 * 1024);

        log.info("[SYSTEM MONITORING] Heap Memory Status -> Used: {}MB / Max: {}MB", usedHeap, maxHeap);
    }
}
