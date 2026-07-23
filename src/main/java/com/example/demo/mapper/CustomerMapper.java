package com.example.demo.mapper;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.example.demo.batch.model.Customer;

/**
 * Customer 테이블 조작을 위한 MyBatis Mapper 인터페이스입니다.
 */
@Mapper
public interface CustomerMapper {

    List<Customer> selectCustomersByStatus(Map<String, Object> params);

    void updateStatuses(@Param("ids") List<Integer> ids, @Param("status") String status);
    void updateStatusSingle(@Param("id") Integer ids);
    List<Customer> selectCustomersByStatusPending();

}
