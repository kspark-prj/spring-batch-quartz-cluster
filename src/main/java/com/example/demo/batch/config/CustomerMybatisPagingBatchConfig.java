package com.example.demo.batch.config;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.demo.batch.model.Customer;
import com.example.demo.batch.model.ProcessedCustomer;
import com.example.demo.mapper.CustomerMapper;
import com.example.demo.mapper.ProcessedCustomerMapper;
import com.example.demo.support.ExternalApiSimulator;

/**
 * Spring Batch 5.x의 메인 Job/Step 설정 파일입니다.
 */
@Configuration
public class CustomerMybatisPagingBatchConfig {

    private static final Logger log = LoggerFactory.getLogger(CustomerMybatisPagingBatchConfig.class);
    private static final int CHUNK_SIZE = 1000;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final SqlSessionFactory sqlSessionFactory;
    private final CustomerMapper customerMapper;
    private final ProcessedCustomerMapper processedCustomerMapper;
    private final ExternalApiSimulator externalApiSimulator;

    // =========================================================================
    // [MULTITHREAD - 1] 멀티스레드 비동기 처리를 위한 TaskExecutor 주입
    // Virtual Threads(가상 스레드) 또는 ThreadPoolTaskExecutor를 주입받아 비동기 처리에 활용합니다.
    // =========================================================================
    private final AsyncTaskExecutor virtualThreadTaskExecutor;

     CustomerMybatisPagingBatchConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SqlSessionFactory sqlSessionFactory,
            CustomerMapper customerMapper,
            ProcessedCustomerMapper processedCustomerMapper,
            ExternalApiSimulator externalApiSimulator,
            @Qualifier("virtualThreadTaskExecutor") AsyncTaskExecutor virtualThreadTaskExecutor) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.sqlSessionFactory = sqlSessionFactory;
        this.customerMapper = customerMapper;
        this.processedCustomerMapper = processedCustomerMapper;
        this.externalApiSimulator = externalApiSimulator;
        this.virtualThreadTaskExecutor = virtualThreadTaskExecutor;
    }

    @Bean(name = "customerMigrationJob")
     Job customerMigrationJob() {
        return new JobBuilder("customerMigrationJob", jobRepository)
                .start(customerMigrationStep())
                .build();
    }

    // =========================================================================
    // [MULTITHREAD - 2] 멀티스레드 구성 전략 선택 (옵션 A vs 옵션 B)
    //
    // [옵션 A] AsyncItemProcessor / AsyncItemWriter 방식 (현재 적용 중)
    //  - Reader는 단일 스레드로 안전하게 읽고, I/O 지연(API 호출 등)이 발생하는 Process 구간만 병렬화합니다.
    //  - Chunk 결과 제네릭 타입으로 Future<T>를 사용합니다.
    //
    // [옵션 B] Multi-threaded Step 방식 (Step 전체 병렬화)
    //  - Step에 .taskExecutor(virtualThreadTaskExecutor)를 추가하면 Chunk(1,000건) 단위로
    //    Read -> Process -> Write 전체 과정을 멀티스레드로 동시 실행합니다.
    //  - MyBatisPagingItemReader는 Thread-safe하므로 옵션 B 적용이 가능합니다.
    //  - 옵션 B 변경 시 AsyncProcessor/Writer 대신 일반 Processor/Writer를 주입하고,
    //    Chunk 제네릭도 <Customer, ProcessedCustomer>로 원복해야 합니다.
    // =========================================================================
    @Bean
    Step customerMigrationStep() {
        return new StepBuilder("customerMigrationStep", jobRepository)
                // [옵션 A 적용 중] Chunk 제네릭에 Future 반환 타입을 설정
                .<Customer, Future<ProcessedCustomer>>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerItemReader())
                .processor(asyncCustomerMybatisPagingProcessor()) // Process 비동기 처리
                .writer(asyncCustomerMybatisPagingWriter())       // Write 비동기 처리

                // [옵션 B로 변경시 주석 해제] Step 단위 전체 멀티스레드 병렬 처리 시 사용
                // .taskExecutor(virtualThreadTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    MyBatisPagingItemReader<Customer> customerItemReader() {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("status", "PENDING");

        return new MyBatisPagingItemReaderBuilder<Customer>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("com.example.demo.mapper.CustomerMapper.selectCustomersByStatus")
                .parameterValues(parameterValues)
                .pageSize(CHUNK_SIZE)

                // =========================================================================
                // [MULTITHREAD - 3] saveState(false) 필수 설정
                // - 멀티스레드나 Async 비동기 환경에서는 스레드가 동시 실행되어 상태(읽기 위치)를
                //   ExecutionContext에 저장 시 경합 및 예외가 발생하므로 false로 오프시킵니다.
                // - MyBatisPagingItemReader는 Thread-safe하므로 saveState(false)로 안전하게 병렬 처리됩니다.
                // =========================================================================
                .saveState(false)
                .build();
    }

    @Bean
     ItemProcessor<Customer, ProcessedCustomer> customerMybatisPagingProcessor() {
        return customer -> {
            log.debug("Processing customer: {}", customer.id());
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
    // [MULTITHREAD - 4] AsyncItemProcessor (가공 로직 멀티스레드 수행)
    // - ItemProcessor의 가공 로직(외부 API 호출)을 TaskExecutor 스레드 풀에 위임하여
    //   I/O 응답 대기 시간 동안 다른 아이템을 병렬 처리합니다.
    // =========================================================================
    @Bean
     AsyncItemProcessor<Customer, ProcessedCustomer> asyncCustomerMybatisPagingProcessor() {
        AsyncItemProcessor<Customer, ProcessedCustomer> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(customerMybatisPagingProcessor());
        asyncProcessor.setTaskExecutor(virtualThreadTaskExecutor); // 가상 스레드 Executor 적용
        return asyncProcessor;
    }

    @Bean
     ItemWriter<ProcessedCustomer> customerMybatisPagingItemWriter() {

        // =========================================================================
        // 1. Target 테이블(processed_customer) 저장용 MyBatisBatchItemWriter 생성
        // =========================================================================
        MyBatisBatchItemWriter<ProcessedCustomer> processedWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.ProcessedCustomerMapper.insertProcessedCustomer")
                .assertUpdates(false)
                .build();

        // =========================================================================
        // 2. Source 테이블(customer) 상태 업데이트용 MyBatisBatchItemWriter 생성
        // =========================================================================
        MyBatisBatchItemWriter<ProcessedCustomer> statusUpdateWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.CustomerMapper.updateStatusSingle")
                .assertUpdates(false)
                .build();

        // =========================================================================
        // 3. Writer 필수 속성 검증 및 초기화
        // =========================================================================
        try {
            processedWriter.afterPropertiesSet();
            statusUpdateWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("MyBatisBatchItemWriter 초기화 실패", e);
        }

        // =========================================================================
        // 4. Actual Chunk Write 실행 (람다식)
        // =========================================================================
        return chunk -> {
            List<? extends ProcessedCustomer> processedList = chunk.getItems();

            if (processedList.isEmpty()) {
                return;
            }

            log.info("Writing batch chunk size: {}", processedList.size());

            processedWriter.write(chunk);
            statusUpdateWriter.write(chunk);
        };
    }

    // =========================================================================
    // [MULTITHREAD - 5] AsyncItemWriter (비동기 결과 동기화)
    // - AsyncItemProcessor가 비동기로 반환한 Future 작업 결과들을 동기화(Unwrap)하여
    //   모든 작업 완료 후 집합(Chunk)으로 단일 ItemWriter로 넘겨 DB 대량 처리합니다.
    // =========================================================================
    @Bean
     AsyncItemWriter<ProcessedCustomer> asyncCustomerMybatisPagingWriter() {
        AsyncItemWriter<ProcessedCustomer> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(customerMybatisPagingItemWriter());
        return asyncWriter;
    }

    // =========================================================================
    // Tasklet 예제 (한 번에 실행되는 배치)
    // =========================================================================

    @Bean(name = "customerMigrationTaskletJob")
     Job customerMigrationTaskletJob() {
        return new JobBuilder("customerMigrationTaskletJob", jobRepository)
                .start(customerMigrationTaskletStep())
                .build();
    }

    @Bean
     Step customerMigrationTaskletStep() {
        return new StepBuilder("customerMigrationTaskletStep", jobRepository)
                .tasklet(customerMigrationTasklet(), transactionManager)
                .build();
    }

    @Bean
     Tasklet customerMigrationTasklet() {
        return (contribution, chunkContext) -> {
            // 1. PENDING 상태의 전체 데이터 한 번에 조회
            List<Customer> pendingCustomers = customerMapper.selectCustomersByStatusPending();
            log.info("[Tasklet] Total pending customers fetched: {}", pendingCustomers.size());

            if (pendingCustomers.isEmpty()) {
                return RepeatStatus.FINISHED;
            }

            log.info("[Tasklet] Successfully processed total {} items.", pendingCustomers.size());
            return RepeatStatus.FINISHED;
        };
    }
}