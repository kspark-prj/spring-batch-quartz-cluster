package com.example.demo.batch.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "processed_customer")
public class JpaProcessedCustomer {

    @Id
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String email;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    @Column(name = "api_response")
    private String apiResult;

    protected JpaProcessedCustomer() {}

    public JpaProcessedCustomer(Long id, String name, String email, LocalDateTime processedAt, String apiResult) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.processedAt = processedAt;
        this.apiResult = apiResult;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getEmail() { return email; }
    public LocalDateTime getProcessedAt() { return processedAt; }
    public String getApiResult() { return apiResult; }
}