package com.example.demo.repository;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import com.example.demo.batch.model.JpaCustomer;

public interface JpaCustomerRepository extends JpaRepository<JpaCustomer, Long> {

    List<JpaCustomer> findByStatus(String status);

    @Modifying
    @Query("UPDATE JpaCustomer c SET c.status = :status WHERE c.id IN :ids")
    void updateStatusForIds(@Param("ids") List<Long> ids, @Param("status") String status);
}