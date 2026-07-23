package com.example.demo.quartz.job;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomerMigrationQuartzJob implements org.quartz.Job {

    private final JobLauncher jobLauncher;

    // 배치 Config에 정의하신 Job Bean 주입
    @Qualifier("customerMigrationTaskletJob")
    private final Job customerMigrationTaskletJob;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            // Spring Batch는 동일한 JobParameter로 재실행이 안 되므로 현재 시간을 추가합니다.
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters();

            log.info("Starting Batch Job: customerMigrationTaskletJob");
            jobLauncher.run(customerMigrationTaskletJob, jobParameters);

        } catch (Exception e) {
            log.error("Failed to execute customerMigrationTaskletJob", e);
            throw new JobExecutionException(e);
        }
    }
}