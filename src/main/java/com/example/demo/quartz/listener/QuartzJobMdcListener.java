package com.example.demo.quartz.listener;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * 모든 Quartz Job 실행 전후로 MDC 컨텍스트를 자동 설정/정리하는 글로벌 리스너입니다.
 * Spring Batch의 JobLoggingListener와 동일한 역할을 Quartz 레벨에서 수행합니다.
 */
@Slf4j
@Component
public class QuartzJobMdcListener implements JobListener {

    private static final String JOB_NAME_KEY = "jobName";
    private static final String LISTENER_NAME = "QuartzJobMdcListener";

    @Override
    public String getName() {
        return LISTENER_NAME;
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        String jobName = context.getJobDetail().getKey().getName();
        MDC.put(JOB_NAME_KEY, jobName);
        log.info("Quartz Job [{}] Started.", jobName);
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
        String jobName = context.getJobDetail().getKey().getName();
        log.warn("Quartz Job [{}] Vetoed.", jobName);
        MDC.clear();
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        try {
            String jobName = context.getJobDetail().getKey().getName();
            if (jobException != null) {
                log.error("Quartz Job [{}] Failed.", jobName, jobException);
            } else {
                log.info("Quartz Job [{}] Finished.", jobName);
            }
        } finally {
            MDC.clear();
        }
    }
}
