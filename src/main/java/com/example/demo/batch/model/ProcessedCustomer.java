package com.example.demo.batch.model;

import java.time.LocalDateTime;

/**
 * 타겟 고객 처리 완료 테이블에 대응되는 Java 21 Record 클래스입니다.
 */
public record ProcessedCustomer(
    Integer id,
    String name,
    String email,
    LocalDateTime processedAt,
    String apiResponse
) {}
