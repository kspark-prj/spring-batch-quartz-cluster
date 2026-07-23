package com.example.demo.quartz.job;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Spring Batch Job을 연동 실행시키는 Quartz Job 구현체입니다.
 */
@Component
@DisallowConcurrentExecution
public class SampleBatchTriggerJob implements org.quartz.Job {

    private static final Logger log = LoggerFactory.getLogger(SampleBatchTriggerJob.class);

    private final JobLauncher jobLauncher;
    private final Job customerMigrationJob;

    public SampleBatchTriggerJob(JobLauncher jobLauncher, 
                                 @Qualifier("customerMigrationJob") Job customerMigrationJob) {
        this.jobLauncher = jobLauncher;
        this.customerMigrationJob = customerMigrationJob;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Quartz Batch Trigger Job started. InstId: {}", context.getFireInstanceId());

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("runTime", System.currentTimeMillis())
                    .toJobParameters();

            var execution = jobLauncher.run(customerMigrationJob, jobParameters);
            log.info("Batch job trigger completed status: {}", execution.getStatus());
        } catch (Exception e) {
            log.error("Failed to run Batch Job inside Quartz execution context", e);
            throw new JobExecutionException("Spring Batch 실행 실패", e);
        }
    }
}
