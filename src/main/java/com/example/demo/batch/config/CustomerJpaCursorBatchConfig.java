package com.example.demo.batch.config;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

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
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.demo.batch.model.JpaCustomer;
import com.example.demo.batch.model.JpaProcessedCustomer;
import com.example.demo.repository.JpaCustomerRepository;
import com.example.demo.support.ExternalApiSimulator;

import jakarta.persistence.EntityManagerFactory;

@Configuration
public class CustomerJpaCursorBatchConfig {

    private static final Logger log = LoggerFactory.getLogger(CustomerJpaCursorBatchConfig.class);
    private static final int CHUNK_SIZE = 1000;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;
    private final JpaCustomerRepository jpaCustomerRepository;
    private final ExternalApiSimulator externalApiSimulator;

    // =========================================================================
    // [MULTITHREAD - 1] 멀티스레드 실행을 위한 TaskExecutor 주입
    // Virtual Threads(가상 스레드) 또는 ThreadPoolTaskExecutor를 주입받아 비동기 처리에 활용합니다.
    // =========================================================================
    private final AsyncTaskExecutor virtualThreadTaskExecutor;

    public CustomerJpaCursorBatchConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            EntityManagerFactory entityManagerFactory,
            JpaCustomerRepository jpaCustomerRepository,
            ExternalApiSimulator externalApiSimulator,
            @Qualifier("virtualThreadTaskExecutor") AsyncTaskExecutor virtualThreadTaskExecutor) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.entityManagerFactory = entityManagerFactory;
        this.jpaCustomerRepository = jpaCustomerRepository;
        this.externalApiSimulator = externalApiSimulator;
        this.virtualThreadTaskExecutor = virtualThreadTaskExecutor;
    }

    @Bean(name = "customerJpaMigrationJob")
    public Job customerJpaMigrationJob() {
        return new JobBuilder("customerJpaMigrationJob", jobRepository)
                .start(customerJpaMigrationStep())
                .build();
    }

    // =========================================================================
    // [MULTITHREAD - 2] Step 멀티스레드 구성 방식
    // - JpaCursorItemReader는 Thread-unsafe하므로 Step 레벨에 .taskExecutor()를 적용하면 안 됩니다.
    // - 대신 Reader는 단일 스레드로 안전하게 읽고, Processor/Writer 구간을 Async로 비동기(멀티스레드) 처리합니다.
    //   (만약 Step 레벨 전체 병렬화가 필요하다면 JpaPagingItemReader를 사용해야 합니다)
    // =========================================================================
    @Bean
    public Step customerJpaMigrationStep() {
        return new StepBuilder("customerJpaMigrationStep", jobRepository)
                // Chunk 타입을 Future<JpaProcessedCustomer>로 지정하여 비동기 처리 결과를 받아옵니다.
                .<JpaCustomer, Future<JpaProcessedCustomer>>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerJpaCursorItemReader())
                .processor(asyncCustomerProcessor()) // 비동기 멀티스레드 Processor 연결
                .writer(asyncCustomerWriter())       // 비동기 결과를 수집 및 저장하는 Writer 연결
                // .taskExecutor(virtualThreadTaskExecutor) <-- 주의: CursorReader 사용 시 주석 해제 금지!
                .build();
    }

    @Bean
    @StepScope
    public JpaCursorItemReader<JpaCustomer> customerJpaCursorItemReader() {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("status", "PENDING");

        return new JpaCursorItemReaderBuilder<JpaCustomer>()
                .name("customerJpaCursorItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT c FROM JpaCustomer c WHERE c.status = :status ORDER BY c.id ASC")
                .parameterValues(parameterValues)
                .saveState(false)
                .build();
    }

    @Bean
    public ItemProcessor<JpaCustomer, JpaProcessedCustomer> customerProcessor() {
        return customer -> {
            log.debug("Processing JPA customer: {}", customer.getId());

            // getId() (Long) -> intValue() 변환
            String apiResult = externalApiSimulator.callExternalValidationApi(customer.getId().intValue(), customer.getEmail());

            return new JpaProcessedCustomer(
                    customer.getId(),
                    customer.getName(),
                    customer.getEmail(),
                    LocalDateTime.now(),
                    apiResult
            );
        };
    }

    // =========================================================================
    // [MULTITHREAD - 3] AsyncItemProcessor (가장 핵심적인 멀티스레드 설정 포인트)
    // - Reader가 1건씩 읽은 데이터를 TaskExecutor(스레드 풀)를 사용하여 비동기/병렬로 가공합니다.
    // - 외부 API 호출과 같은 I/O Bound 작업에서 병렬 스레드를 활용해 엄청난 속도 향상을 얻을 수 있습니다.
    // =========================================================================
    @Bean
    public AsyncItemProcessor<JpaCustomer, JpaProcessedCustomer> asyncCustomerProcessor() {
        AsyncItemProcessor<JpaCustomer, JpaProcessedCustomer> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(customerProcessor()); // 동시 처리할 원본 Processor 지정
        asyncProcessor.setTaskExecutor(virtualThreadTaskExecutor); // 병렬 작업을 수행할 멀티스레드 Executor 설정
        return asyncProcessor;
    }

    @Bean
    public ItemWriter<JpaProcessedCustomer> customerItemWriter() {
        JpaItemWriter<JpaProcessedCustomer> processedWriter = new JpaItemWriterBuilder<JpaProcessedCustomer>()
                .entityManagerFactory(entityManagerFactory)
                .build();

        try {
            processedWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("JpaItemWriter 초기화 실패", e);
        }

        return chunk -> {
            if (chunk.isEmpty()) {
                return;
            }

            // 1. ProcessedCustomer 저장 (JpaItemWriter 실행)
            processedWriter.write(chunk);

            // 2. ID 추출 (람다 표현식 활용)
            List<Long> customerIds = chunk.getItems().stream()
                    .map(item -> item.getId())
                    .toList();

            log.info("[JPA Chunk] Writing batch chunk size: {}", customerIds.size());

            // 3. 원본 Customer status 일괄 업데이트
            jpaCustomerRepository.updateStatusForIds(customerIds, "PROCESSED");
        };
    }

    // =========================================================================
    // [MULTITHREAD - 4] AsyncItemWriter
    // - AsyncItemProcessor에서 비동기로 반환된 Future 객체들을 Unwrap(동기화)하여
    //   모든 비동기 작업이 완료될 때까지 기다렸다가 수집된 결과를 단일 Chunk로 묶어 ItemWriter로 전달합니다.
    // =========================================================================
    @Bean
    public AsyncItemWriter<JpaProcessedCustomer> asyncCustomerWriter() {
        AsyncItemWriter<JpaProcessedCustomer> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(customerItemWriter()); // 최종 저장 처리를 담당할 Delegate 지정
        return asyncWriter;
    }

    // Tasklet 예제
    @Bean(name = "customerJpaMigrationTaskletJob")
    public Job customerJpaMigrationTaskletJob() {
        return new JobBuilder("customerJpaMigrationTaskletJob", jobRepository)
                .start(customerJpaMigrationStep())
                .build();
    }

    @Bean
    public Step customerJpaMigrationTaskletStep() {
        return new StepBuilder("customerJpaMigrationTaskletStep", jobRepository)
                .tasklet(customerJpaMigrationTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet customerJpaMigrationTasklet() {
        return (contribution, chunkContext) -> {
            List<JpaCustomer> pendingCustomers = jpaCustomerRepository.findByStatus("PENDING");
            log.info("[JPA Tasklet] Total pending customers fetched: {}", pendingCustomers.size());

            if (pendingCustomers.isEmpty()) {
                return RepeatStatus.FINISHED;
            }

            log.info("[JPA Tasklet] Successfully processed total {} items.", pendingCustomers.size());
            return RepeatStatus.FINISHED;
        };
    }
}