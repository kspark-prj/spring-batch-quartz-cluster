package com.example.demo.quartz.dto;

import java.util.Map;

/**
 * Quartz Job을 동적으로 등록할 때 사용되는 Request DTO (Java 21 Record)
 */
public record JobRequest(
    String jobName,
    String jobGroup,
    String cronExpression,
    Map<String, Object> jobData
) {}
