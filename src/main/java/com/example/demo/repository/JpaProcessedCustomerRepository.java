package com.example.demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.demo.batch.model.JpaProcessedCustomer;

public interface JpaProcessedCustomerRepository extends JpaRepository<JpaProcessedCustomer, Long> {
}