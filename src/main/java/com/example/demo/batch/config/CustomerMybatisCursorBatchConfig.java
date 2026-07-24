package com.example.demo.batch.config;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisCursorItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisCursorItemReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.demo.batch.model.Customer;
import com.example.demo.batch.model.ProcessedCustomer;
import com.example.demo.support.ExternalApiSimulator;

/**
 * MyBatisCursorItemReader 기반 Spring Batch 5.x 설정 클래스입니다.
 */
@Configuration
public class CustomerMybatisCursorBatchConfig {

    private static final Logger log = LoggerFactory.getLogger(CustomerMybatisCursorBatchConfig.class);
    private static final int CHUNK_SIZE = 1000;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final SqlSessionFactory sqlSessionFactory;
    private final ExternalApiSimulator externalApiSimulator;

    // =========================================================================
    // [MULTITHREAD - 1] 멀티스레드 비동기 처리를 위한 TaskExecutor 주입
    // Virtual Threads(가상 스레드) 또는 ThreadPoolTaskExecutor를 주입받아 비동기 처리에 활용합니다.
    // =========================================================================
    private final AsyncTaskExecutor virtualThreadTaskExecutor;

    public CustomerMybatisCursorBatchConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SqlSessionFactory sqlSessionFactory,
            ExternalApiSimulator externalApiSimulator,
            @Qualifier("virtualThreadTaskExecutor") AsyncTaskExecutor virtualThreadTaskExecutor) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.sqlSessionFactory = sqlSessionFactory;
        this.externalApiSimulator = externalApiSimulator;
        this.virtualThreadTaskExecutor = virtualThreadTaskExecutor;
    }

    @Bean(name = "customerCursorMigrationJob")
    Job customerCursorMigrationJob() {
        return new JobBuilder("customerCursorMigrationJob", jobRepository)
                .start(customerCursorMigrationStep())
                .build();
    }

    // =========================================================================
    // [MULTITHREAD - 2] Step 구조 및 멀티스레드 사용 전략
    // - MyBatisCursorItemReader는 Thread-unsafe하므로 Step 레벨에 .taskExecutor()를 추가하면 안 됩니다.
    // - Reader는 단일 스레드로 안전하게 데이터를 커서 스트리밍 방식으로 읽고,
    //   AsyncItemProcessor / AsyncItemWriter를 통해 가공(Process) 단계만 비동기/병렬 스레드로 실행합니다.
    // =========================================================================
    @Bean
    Step customerCursorMigrationStep() {
        return new StepBuilder("customerCursorMigrationStep", jobRepository)
                // Async 처리를 위해 Chunk 결과 타입을 Future<ProcessedCustomer>로 지정
                .<Customer, Future<ProcessedCustomer>>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerMybatisCursorItemReader())
                .processor(asyncCustomerMybatisCursorProcessor()) // 멀티스레드 비동기 Processor
                .writer(asyncCustomerMybatisCursorWriter())       // 비동기 결과 취합/저장 Writer
                // .taskExecutor(virtualThreadTaskExecutor) <-- 주의: MyBatis Cursor Reader 사용 시 주석 해제 금지!
                .build();
    }

    /**
     * MyBatisCursorItemReader 설정
     * - DB 커서를 생성하여 스트리밍 방식으로 데이터를 페치합니다.
     * - Thread-Safe하지 않으므로 Step 자체를 멀티 스레드로 돌려선 안 됩니다. (AsyncItemProcessor 사용은 가능)
     */
    @Bean
    @StepScope
    MyBatisCursorItemReader<Customer> customerMybatisCursorItemReader() {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("status", "PENDING");

        return new MyBatisCursorItemReaderBuilder<Customer>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("com.example.demo.mapper.CustomerMapper.selectCustomersByStatusCursor")
                .parameterValues(parameterValues)

                // =========================================================================
                // [MULTITHREAD - 3] saveState(false) 설정
                // - 비동기/멀티스레드 환경에서는 여러 스레드 및 작업 상태의 타이밍이 달라지므로
                //   ExecutionCtx에 읽기 진행 상태를 저장하지 않도록 false로 설정합니다.
                // =========================================================================
                .saveState(false) // 배치 재시작 상태를 저장하지 않을 경우 false 설정
                .build();
    }

    @Bean
    ItemProcessor<Customer, ProcessedCustomer> customerMybatisCursorProcessor() {
        return customer -> {
            log.debug("Processing customer via Cursor: {}", customer.id());
            String apiResult = externalApiSimulator.callExternalValidationApi(customer.id(), customer.email());
            return new ProcessedCustomer(
                    customer.id(),
                    customer.name(),
                    customer.email(),
                    LocalDateTime.now(),
                    apiResult
            );
        };
    }

    // =========================================================================
    // [MULTITHREAD - 4] AsyncItemProcessor (MyBatis Cursor 배치 핵심 병렬화)
    // - Reader가 1건씩 스트리밍해온 데이터의 가공 작업(외부 API 호출)을
    //   TaskExecutor(Virtual Thread 등) 스레드 풀에 위임하여 동시 병렬로 수행합니다.
    // =========================================================================
    @Bean
    AsyncItemProcessor<Customer, ProcessedCustomer> asyncCustomerMybatisCursorProcessor() {
        AsyncItemProcessor<Customer, ProcessedCustomer> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(customerMybatisCursorProcessor());
        asyncProcessor.setTaskExecutor(virtualThreadTaskExecutor); // 가상 스레드 Executor 전달
        return asyncProcessor;
    }

    @Bean
    public ItemWriter<ProcessedCustomer> customerMybatisItemWriter() {

        // 1. Target 테이블(processed_customer) 저장용 MyBatisBatchItemWriter
        MyBatisBatchItemWriter<ProcessedCustomer> processedWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.ProcessedCustomerMapper.insertProcessedCustomer")
                .assertUpdates(false)
                .build();

        // 2. Source 테이블(customer) 상태 업데이트용 MyBatisBatchItemWriter
        MyBatisBatchItemWriter<ProcessedCustomer> statusUpdateWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.CustomerMapper.updateStatusSingle")
                .assertUpdates(false)
                .build();

        // 3. Writer 초기화 검증
        try {
            processedWriter.afterPropertiesSet();
            statusUpdateWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("MyBatisBatchItemWriter 초기화 실패", e);
        }

        // 4. Batch Write 실행
        return chunk -> {
            List<? extends ProcessedCustomer> processedList = chunk.getItems();

            if (processedList.isEmpty()) {
                return;
            }

            log.info("[Cursor Job] Writing batch chunk size: {}", processedList.size());

            processedWriter.write(chunk);
            statusUpdateWriter.write(chunk);
        };
    }

    // =========================================================================
    // [MULTITHREAD - 5] AsyncItemWriter
    // - AsyncItemProcessor에서 비동기로 반환받은 Future 결과를 동기화(Unwrap)하여
    //   Chunk 크기만큼 완료되길 기다린 후, 한 번에 MyBatis Batch Writer로 전달하여 DB에 집단 처리합니다.
    // =========================================================================
    @Bean
    public AsyncItemWriter<ProcessedCustomer> asyncCustomerMybatisCursorWriter() {
        AsyncItemWriter<ProcessedCustomer> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(customerMybatisItemWriter());
        return asyncWriter;
    }
}