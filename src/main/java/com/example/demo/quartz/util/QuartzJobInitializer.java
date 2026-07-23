package com.example.demo.quartz.util;

import java.util.Map;

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Component;

import com.example.demo.quartz.dto.JobRequest;
import com.example.demo.quartz.job.CustomerMigrationQuartzJob;
import com.example.demo.quartz.job.SampleBatchTriggerJob;
import com.example.demo.quartz.job.SampleSystemMonitoringJob;
import com.example.demo.quartz.service.QuartzJobService;

/**
 * 구동 시 기본 스케줄(Monitoring 및 Batch Trigger)을 자동으로 등록해 주는 이니셜라이저입니다.
 */
@Component
public class QuartzJobInitializer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(QuartzJobInitializer.class);

    private final QuartzJobService quartzJobService;
    private final Scheduler scheduler;

    public QuartzJobInitializer(QuartzJobService quartzJobService, SchedulerFactoryBean schedulerFactoryBean) {
        this.quartzJobService = quartzJobService;
        this.scheduler = schedulerFactoryBean.getScheduler();
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Quartz 기본 크론 스케줄링 검사 및 자동 등록 시작...");

        JobKey monitorKey = new JobKey("DefaultSystemMonitoringJob", "MONITOR_GROUP");
        if (!scheduler.checkExists(monitorKey)) {
            JobRequest request = new JobRequest(
                    "DefaultSystemMonitoringJob",
                    "MONITOR_GROUP",
                    "0/30 * * * * ?",
                    Map.of("desc", "System health monitor")
            );
            quartzJobService.addJob(request, SampleSystemMonitoringJob.class);
            log.info("DefaultSystemMonitoringJob이 정상 등록되었습니다.");
        }

        JobKey batchKey = new JobKey("DefaultBatchTriggerJob", "BATCH_GROUP");
        if (!scheduler.checkExists(batchKey)) {
            JobRequest request = new JobRequest(
                    "DefaultBatchTriggerJob",
                    "BATCH_GROUP",
                    "0 0/5 * * * ?",
                    Map.of("runBy", "System Auto Initializer")
            );
            quartzJobService.addJob(request, SampleBatchTriggerJob.class);
            log.info("DefaultBatchTriggerJob이 정상 등록되었습니다.");
        }

        JobKey taskletKey = new JobKey("CustomerMigrationTaskletJob", "BATCH_GROUP");

        if (!scheduler.checkExists(taskletKey)) {
            JobRequest request = new JobRequest(
                    "CustomerMigrationTaskletJob",
                    "BATCH_GROUP",
                    "0 0/2 * * * ?",  // 원하는 Cron 표현식 (예: 2분마다 실행)
                    Map.of("runBy", "Customer migration tasklet monitor")
            );

            // 위에서 작성한 CustomerMigrationQuartzJob 클래스를 지정합니다.
            quartzJobService.addJob(request, CustomerMigrationQuartzJob.class);
        }
        log.info("Quartz 스케줄링 자동 초기화 완료.");
    }
}
