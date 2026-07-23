package com.example.demo.config;

import java.util.concurrent.Executors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;

/**
 * Java 21 Virtual Threads를 스프링 TaskExecutor로 지정하기 위한 설정 클래스입니다.
 */
@Configuration
public class ThreadConfig {

    /**
     * Virtual Thread TaskExecutor 빈 등록
     */
    @Bean(name = "virtualThreadTaskExecutor")
    AsyncTaskExecutor virtualThreadTaskExecutor() {
        return new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
    }
}
