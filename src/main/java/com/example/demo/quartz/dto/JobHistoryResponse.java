package com.example.demo.quartz.dto;

/**
 * Job 실행 이력을 표현하는 Response DTO입니다.
 * 그리드 뷰 및 차트 시각화에 사용됩니다.
 */
public record JobHistoryResponse(
    String jobName,
    String jobGroup,
    String status,
    String startTime,
    String endTime,
    long durationMs,
    String instanceId
) {}
