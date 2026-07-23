package com.example.demo.batch.model;

import java.time.LocalDateTime;

/**
 * 소스 고객 테이블에 대응되는 Java 21 Record 클래스입니다.
 */
public record Customer(
    Integer id,
    String name,
    String email,
    String status,
    LocalDateTime createdAt
) {}
