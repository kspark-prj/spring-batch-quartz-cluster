package com.example.demo.quartz.dto;

import java.util.List;

/**
 * Quartz 스케줄러 자체의 구동 상태 및 등록된 Job 목록 전체를 감싸서 리턴하는 Response DTO (Java 21 Record)
 */
public record SchedulerStatusResponse(
    String schedulerName,
    String schedulerInstanceId,
    boolean isStarted,
    boolean isInStandbyMode,
    List<JobResponse> registeredJobs
) {}
