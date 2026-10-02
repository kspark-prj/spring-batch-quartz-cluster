package com.example.demo.batch.listener;

import org.slf4j.MDC;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Step 실행 전후로 Step 레벨의 MDC 컨텍스트를 관리하는 리스너입니다.
 * jobName MDC는 QuartzJobMdcListener에서 담당하며, 여기서는 stepName만 관리합니다.
 */
@Slf4j
@Component
public class StepLoggingListener implements StepExecutionListener {

    private static final String STEP_NAME_KEY = "stepName";

    @Override
    public void beforeStep(StepExecution stepExecution) {
        String stepName = stepExecution.getStepName();
        MDC.put(STEP_NAME_KEY, stepName);
        log.info("Step [{}] Started.", stepName);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        try {
            log.info("Step [{}] Finished with status: {}",
                    stepExecution.getStepName(),
                    stepExecution.getExitStatus().getExitCode());
            return stepExecution.getExitStatus();
        } finally {
            // Job 컨텍스트(jobName)는 유지하고 Step 컨텍스트만 정밀 제거
            MDC.remove(STEP_NAME_KEY);
        }
    }
}