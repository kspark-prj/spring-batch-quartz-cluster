package com.example.demo.batch.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Job 실행 전후로 로그를 출력하는 리스너입니다.
 * MDC 컨텍스트 관리는 QuartzJobMdcListener에서 담당합니다.
 */
@Slf4j
@Component
public class JobLoggingListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        log.info("Job [{}] Started.", jobName);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        log.info("Job [{}] Finished with status: {}", jobName, jobExecution.getStatus());
    }
}