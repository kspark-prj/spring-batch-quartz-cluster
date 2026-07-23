package com.example.demo.quartz.service;

import com.example.demo.quartz.dto.JobRequest;
import com.example.demo.quartz.dto.JobResponse;
import com.example.demo.quartz.dto.SchedulerStatusResponse;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Quartz Job을 동적으로 조회, 추가, 정지, 재개, 실행 및 삭제하는 비즈니스 로직 서비스입니다.
 */
@Service
public class QuartzJobService {

    private static final Logger log = LoggerFactory.getLogger(QuartzJobService.class);
    private final Scheduler scheduler;

    public QuartzJobService(SchedulerFactoryBean schedulerFactoryBean) {
        this.scheduler = schedulerFactoryBean.getScheduler();
    }

    public SchedulerStatusResponse getSchedulerStatus() {
        try {
            List<JobResponse> jobs = new ArrayList<>();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.anyJobGroup())) {
                JobDetail jobDetail = scheduler.getJobDetail(jobKey);
                List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);

                String cronExpr = "N/A";
                String nextFireStr = "N/A";
                String prevFireStr = "N/A";
                String triggerState = "NONE";

                if (!triggers.isEmpty()) {
                    Trigger trigger = triggers.get(0);
                    TriggerKey triggerKey = trigger.getKey();
                    triggerState = scheduler.getTriggerState(triggerKey).name();

                    if (trigger instanceof CronTrigger cronTrigger) {
                        cronExpr = cronTrigger.getCronExpression();
                    }
                    if (trigger.getNextFireTime() != null) {
                        nextFireStr = sdf.format(trigger.getNextFireTime());
                    }
                    if (trigger.getPreviousFireTime() != null) {
                        prevFireStr = sdf.format(trigger.getPreviousFireTime());
                    }
                }

                jobs.add(new JobResponse(
                        jobKey.getName(),
                        jobKey.getGroup(),
                        triggerState,
                        cronExpr,
                        nextFireStr,
                        prevFireStr,
                        jobDetail.getDescription()
                ));
            }

            return new SchedulerStatusResponse(
                    scheduler.getSchedulerName(),
                    scheduler.getSchedulerInstanceId(),
                    scheduler.isStarted(),
                    scheduler.isInStandbyMode(),
                    jobs
            );
        } catch (SchedulerException e) {
            log.error("Failed to retrieve scheduler status", e);
            throw new RuntimeException("Quartz 스케줄러 상태 조회 실패", e);
        }
    }

    public boolean addJob(JobRequest request, Class<? extends Job> jobClass) {
        try {
            JobKey jobKey = new JobKey(request.jobName(), request.jobGroup());
            TriggerKey triggerKey = new TriggerKey(request.jobName() + "_trigger", request.jobGroup());

            if (scheduler.checkExists(jobKey)) {
                log.warn("Job already exists: {}", jobKey);
                return false;
            }

            JobDataMap jobDataMap = new JobDataMap();
            if (request.jobData() != null) {
                jobDataMap.putAll(request.jobData());
            }

            JobDetail jobDetail = JobBuilder.newJob(jobClass)
                    .withIdentity(jobKey)
                    .withDescription("Dynamically registered cron job via REST API")
                    .usingJobData(jobDataMap)
                    .storeDurably()
                    .build();

            CronTrigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(triggerKey)
                    .withSchedule(CronScheduleBuilder.cronSchedule(request.cronExpression())
                            .withMisfireHandlingInstructionDoNothing())
                    .build();

            scheduler.scheduleJob(jobDetail, trigger);
            log.info("Successfully scheduled Job: {} with Cron: {}", jobKey, request.cronExpression());
            return true;
        } catch (SchedulerException e) {
            log.error("Failed to register dynamic job", e);
            throw new RuntimeException("Quartz Job 등록 실패", e);
        }
    }

    /**
     * 기존 등록된 Job의 크론 표현식을 수정합니다.
     */
    public boolean updateCronExpression(String jobName, String jobGroup, String newCronExpression) {
        try {
            TriggerKey triggerKey = new TriggerKey(jobName + "_trigger", jobGroup);

            if (!scheduler.checkExists(triggerKey)) {
                log.warn("Trigger does not exist for update: {}", triggerKey);
                return false;
            }

            CronTrigger newTrigger = TriggerBuilder.newTrigger()
                    .withIdentity(triggerKey)
                    .withSchedule(CronScheduleBuilder.cronSchedule(newCronExpression)
                            .withMisfireHandlingInstructionDoNothing())
                    .build();

            Date rescheduledTime = scheduler.rescheduleJob(triggerKey, newTrigger);
            if (rescheduledTime != null) {
                log.info("Successfully updated Cron expression for Trigger: {} to [{}]. Next fire time: {}",
                        triggerKey, newCronExpression, rescheduledTime);
                return true;
            } else {
                log.warn("Failed to reschedule job for Trigger: {}", triggerKey);
                return false;
            }
        } catch (SchedulerException e) {
            log.error("Failed to update cron expression", e);
            throw new RuntimeException("Quartz Job 크론 표현식 변경 실패", e);
        }
    }

    public boolean deleteJob(String jobName, String jobGroup) {
        try {
            JobKey jobKey = new JobKey(jobName, jobGroup);
            if (scheduler.checkExists(jobKey)) {
                boolean result = scheduler.deleteJob(jobKey);
                log.info("Job deletion execution. Key: {}, Result: {}", jobKey, result);
                return result;
            }
            return false;
        } catch (SchedulerException e) {
            log.error("Failed to delete job", e);
            throw new RuntimeException("Quartz Job 삭제 실패", e);
        }
    }

    public void pauseJob(String jobName, String jobGroup) {
        try {
            JobKey jobKey = new JobKey(jobName, jobGroup);
            scheduler.pauseJob(jobKey);
            log.info("Job successfully PAUSED. Key: {}", jobKey);
        } catch (SchedulerException e) {
            log.error("Failed to pause job", e);
            throw new RuntimeException("Quartz Job Pause 실패", e);
        }
    }

    public void resumeJob(String jobName, String jobGroup) {
        try {
            JobKey jobKey = new JobKey(jobName, jobGroup);
            scheduler.resumeJob(jobKey);
            log.info("Job successfully RESUMED. Key: {}", jobKey);
        } catch (SchedulerException e) {
            log.error("Failed to resume job", e);
            throw new RuntimeException("Quartz Job Resume 실패", e);
        }
    }

    public void triggerJob(String jobName, String jobGroup, Map<String, Object> extraParams) {
        try {
            JobKey jobKey = new JobKey(jobName, jobGroup);
            JobDataMap jobDataMap = new JobDataMap();
            if (extraParams != null) {
                jobDataMap.putAll(extraParams);
            }
            scheduler.triggerJob(jobKey, jobDataMap);
            log.info("Job immediately triggered manually. Key: {}", jobKey);
        } catch (SchedulerException e) {
            log.error("Failed to trigger job manually", e);
            throw new RuntimeException("Quartz Job 즉시 실행 실패", e);
        }
    }
}
