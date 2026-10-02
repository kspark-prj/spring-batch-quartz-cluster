package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.example.demo.batch.listener.MdcTaskDecorator;

@Configuration
public class ThreadConfig {

	/**
     * Batch 전용 Platform Thread Pool TaskExecutor 빈 등록
     */
    @Bean(name = "batchTaskExecutor")
    AsyncTaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // CPU 코어 수 및 DB Connection Pool 규모에 맞춘 스레드 풀 설정
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("batch-exec-");

        // MDC 전파를 위한 Decorator 설정
        executor.setTaskDecorator(new MdcTaskDecorator());

        // 애플리케이션 종료 시 수행 중인 Batch 작업 완료 대기
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);

        executor.initialize();
        return executor;
    }
}
