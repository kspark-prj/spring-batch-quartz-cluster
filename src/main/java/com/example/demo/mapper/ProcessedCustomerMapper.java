package com.example.demo.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.example.demo.batch.model.ProcessedCustomer;

/**
 * ProcessedCustomer 테이블 조작을 위한 MyBatis Mapper 인터페이스입니다.
 */
@Mapper
public interface ProcessedCustomerMapper {

    void insertProcessedCustomersBulk(@Param("list") List<ProcessedCustomer> list);
    void insertProcessedCustomer(ProcessedCustomer processedCustomer);
}
