package com.example.demo.dummy;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 어플리케이션 구동 시 10만 건의 더미 고객 데이터를 고속 벌크 인서트하는 컴포넌트입니다.
 */
@Component
public class DummyDataGenerator implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DummyDataGenerator.class);
    private final JdbcTemplate jdbcTemplate;

    public DummyDataGenerator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        Integer customerCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customer", Integer.class);
        if (customerCount != null && customerCount > 0) {
            log.info("더미 데이터가 이미 존재합니다. (총 {}건). 생성을 건너뜁니다.", customerCount);
            return;
        }

        log.info("더미 데이터 생성을 시작합니다. target: 100,000건");
        long start = System.currentTimeMillis();

        String sql = "INSERT INTO customer (name, email, status) VALUES (?, ?, ?)";
        int totalRecords = 100000;
        int batchSize = 5000;

        List<Object[]> batchArgs = new ArrayList<>(batchSize);

        for (int i = 1; i <= totalRecords; i++) {
            batchArgs.add(new Object[] {
                    "UserName_" + i,
                    "user_" + i + "@example.com",
                    "PENDING"
            });

            if (i % batchSize == 0) {
                jdbcTemplate.batchUpdate(sql, batchArgs);
                batchArgs.clear();
                log.info("벌크 데이터 적재 중... {}/{} 건 완료", i, totalRecords);
            }
        }

        if (!batchArgs.isEmpty()) {
            jdbcTemplate.batchUpdate(sql, batchArgs);
        }

        long duration = System.currentTimeMillis() - start;
        log.info("10만 건의 더미 데이터 적재가 완료되었습니다. 소요 시간: {}ms", duration);
    }
}
