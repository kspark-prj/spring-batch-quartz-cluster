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
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
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

/**
 * JpaPagingItemReader 기반 Spring Batch 5.x 설정 파일입니다.
 */
@Configuration
public class CustomerJpaPagingBatchConfig {

    private static final Logger log = LoggerFactory.getLogger(CustomerJpaPagingBatchConfig.class);
    private static final int CHUNK_SIZE = 1000;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;
    private final JpaCustomerRepository jpaCustomerRepository;
    private final ExternalApiSimulator externalApiSimulator;

    // =========================================================================
    // [MULTITHREAD - 1] 멀티스레드 비동기 처리를 위한 TaskExecutor 주입
    // Virtual Threads(가상 스레드) 또는 ThreadPoolTaskExecutor를 주입받아 사용합니다.
    // =========================================================================
    private final AsyncTaskExecutor virtualThreadTaskExecutor;

    public CustomerJpaPagingBatchConfig(
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

    @Bean(name = "customerJpaPagingMigrationJob")
    public Job customerJpaPagingMigrationJob() {
        return new JobBuilder("customerJpaPagingMigrationJob", jobRepository)
                .start(customerJpaPagingMigrationStep())
                .build();
    }

    // =========================================================================
    // [MULTITHREAD - 2] 멀티스레드 방식 선택 (옵션 A vs 옵션 B)
    //
    // [옵션 A] AsyncItemProcessor / AsyncItemWriter 방식 (현재 적용 중)
    //  - Read는 단일 스레드로 진행하고, 외부 API 호출 등 Process 작업만 병렬화합니다.
    //  - Generic 타입으로 Future<T>를 사용합니다.
    //
    // [옵션 B] Multi-threaded Step 방식 (Step 전체 병렬화)
    //  - Step에 .taskExecutor(virtualThreadTaskExecutor)를 추가하면 Chunk(1,000건) 단위로
    //    Read -> Process -> Write 전체 과정을 멀티스레드로 동시 실행합니다.
    //  - 이때는 AsyncProcessor/Writer 대신 일반 Processor/Writer를 지정하고,
    //    Chunk 제네릭도 <JpaCustomer, JpaProcessedCustomer>로 원복해야 합니다.
    // =========================================================================
    @Bean
    public Step customerJpaPagingMigrationStep() {
        return new StepBuilder("customerJpaPagingMigrationStep", jobRepository)
                // [옵션 A 적용 중] Chunk 제네릭에 Future 반환 타입을 설정
                .<JpaCustomer, Future<JpaProcessedCustomer>>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerJpaPagingItemReader())
                .processor(asyncCustomerJpaProcessor()) // Process 비동기 처리
                .writer(asyncCustomerJpaWriter())       // Write 비동기 처리

                // [옵션 B로 변경시 주석 해제] Step 단위 전체 멀티스레드 병렬 처리 시 사용
                // .taskExecutor(virtualThreadTaskExecutor)
                .build();
    }

    /**
     * JPA Paging 기반 Reader
     * - pageSize 단위로 JPQL 페이지 쿼리(OFFSET/LIMIT)를 실행합니다.
     * - CHUNK_SIZE와 pageSize를 동일하게 1000으로 설정해야 최적화됩니다.
     */
    @Bean
    @StepScope
    public JpaPagingItemReader<JpaCustomer> customerJpaPagingItemReader() {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("status", "PENDING");

        return new JpaPagingItemReaderBuilder<JpaCustomer>()
                .name("customerJpaPagingItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT c FROM JpaCustomer c WHERE c.status = :status ORDER BY c.id ASC")
                .parameterValues(parameterValues)
                .pageSize(CHUNK_SIZE) // 페이징 사이즈를 Chunk 사이즈와 동일하게 맞춤

                // =========================================================================
                // [MULTITHREAD - 3] saveState(false) 설정 필수!
                // - 멀티스레드 환경이나 Async 환경에서는 여러 스레드가 동시에 읽거나
                //   비동기로 처리되므로 ExecutionContext에 상태(읽은 위치 등)를 저장하면 안 됩니다.
                // - JpaPagingItemReader는 Thread-safe하므로 saveState(false) 설정 시 안전합니다.
                // =========================================================================
                .saveState(false)
                .build();
    }

    @Bean
    public ItemProcessor<JpaCustomer, JpaProcessedCustomer> customerPagingProcessor() {
        return customer -> {
            log.debug("Processing JPA customer: {}", customer.getId());

            String apiResult = externalApiSimulator.callExternalValidationApi(
                    customer.getId().intValue(),
                    customer.getEmail()
            );

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
    // [MULTITHREAD - 4] AsyncItemProcessor (가공 작업 병렬화)
    // - ItemProcessor의 로직(외부 API 호출 등 I/O 지연 작업)을 TaskExecutor 스레드에서
    //   비동기로 개별 실행하여 병렬 속도를 대폭 끌어올립니다.
    // =========================================================================
    @Bean
    public AsyncItemProcessor<JpaCustomer, JpaProcessedCustomer> asyncCustomerJpaProcessor() {
        AsyncItemProcessor<JpaCustomer, JpaProcessedCustomer> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(customerPagingProcessor()); // 실제 가공 로직 지정
        asyncProcessor.setTaskExecutor(virtualThreadTaskExecutor); // 가상 스레드 Executor 전달
        return asyncProcessor;
    }

    @Bean
    public ItemWriter<JpaProcessedCustomer> customerJpaItemWriter() {
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

            // 1. Target 테이블 저장 (JpaItemWriter)
            processedWriter.write(chunk);

            // 2. ID 추출 (람다 활용)
            List<Long> customerIds = chunk.getItems().stream()
                    .map(item -> item.getId())
                    .toList();

            log.info("[JPA Paging Chunk] Writing batch chunk size: {}", customerIds.size());

            // 3. Source 테이블 status 일괄 업데이트
            jpaCustomerRepository.updateStatusForIds(customerIds, "PROCESSED");
        };
    }

    // =========================================================================
    // [MULTITHREAD - 5] AsyncItemWriter (비동기 결과 취합 후 저장)
    // - AsyncItemProcessor에서 반환된 Future 객체들이 완료될 때까지 기다렸다가
    //   실제 ItemWriter(customerJpaItemWriter)로 보냅니다.
    // =========================================================================
    @Bean
    public AsyncItemWriter<JpaProcessedCustomer> asyncCustomerJpaWriter() {
        AsyncItemWriter<JpaProcessedCustomer> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(customerJpaItemWriter());
        return asyncWriter;
    }

    // Tasklet 예제
    @Bean(name = "customerJpaPagingTaskletJob")
    public Job customerJpaPagingTaskletJob() {
        return new JobBuilder("customerJpaPagingTaskletJob", jobRepository)
                .start(customerJpaPagingTaskletStep())
                .build();
    }

    @Bean
    public Step customerJpaPagingTaskletStep() {
        return new StepBuilder("customerJpaPagingTaskletStep", jobRepository)
                .tasklet(customerJpaPagingTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet customerJpaPagingTasklet() {
        return (contribution, chunkContext) -> {
            List<JpaCustomer> pendingCustomers = jpaCustomerRepository.findByStatus("PENDING");
            log.info("[JPA Paging Tasklet] Total pending customers fetched: {}", pendingCustomers.size());

            if (pendingCustomers.isEmpty()) {
                return RepeatStatus.FINISHED;
            }

            log.info("[JPA Paging Tasklet] Successfully processed total {} items.", pendingCustomers.size());
            return RepeatStatus.FINISHED;
        };
    }
}