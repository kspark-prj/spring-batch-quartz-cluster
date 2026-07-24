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

	// 1. 기존 페이징용 메서드 (MyBatisPagingItemReader에서 사용)
    List<Customer> selectCustomersByStatusPaging(Map<String, Object> params);

    // 2. [추가] 커서용 메서드 (MyBatisCursorItemReader에서 사용)
    List<Customer> selectCustomersByStatusCursor(Map<String, Object> params);

    void updateStatuses(@Param("ids") List<Integer> ids, @Param("status") String status);
    void updateStatusSingle(@Param("id") Integer ids);
    List<Customer> selectCustomersByStatusPending();

    // =========================================================================
    // [Partitioning 전용 메서드]
    // =========================================================================

    /**
     * 특정 상태(status)의 최소 PK ID를 조회합니다.
     */
    long selectMinIdByStatus(@Param("status") String status);

    /**
     * 특정 상태(status)의 최대 PK ID를 조회합니다.
     */
    long selectMaxIdByStatus(@Param("status") String status);

    /**
     * 특정 상태(status) 및 PK ID 범위(minId ~ maxId) 조건의 데이터를 조회합니다. (Paging 매핑)
     */
    List<Customer> selectCustomersByStatusAndIdRange(
            @Param("status") String status,
            @Param("minId") Long minId,
            @Param("maxId") Long maxId
    );
}
