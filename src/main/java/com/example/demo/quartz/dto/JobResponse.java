package com.example.demo.quartz.dto;

/**
 * 특정 Quartz Job의 상태 정보를 제공하는 Response DTO (Java 21 Record)
 */
public record JobResponse(
    String jobName,
    String jobGroup,
    String triggerState,
    String cronExpression,
    String nextFireTime,
    String previousFireTime,
    String description
) {}
