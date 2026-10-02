package com.example.demo.batch.config;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

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

import com.example.demo.batch.listener.JobLoggingListener;
import com.example.demo.batch.listener.StepLoggingListener; // [추가] StepLoggingListener import
import com.example.demo.batch.model.JpaCustomer;
import com.example.demo.batch.model.JpaProcessedCustomer;
import com.example.demo.repository.JpaCustomerRepository;
import com.example.demo.support.ExternalApiSimulator;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class CustomerJpaCursorBatchConfig {

    private static final int CHUNK_SIZE = 1000;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;
    private final JpaCustomerRepository jpaCustomerRepository;
    private final ExternalApiSimulator externalApiSimulator;
    private final AsyncTaskExecutor batchTaskExecutor;
    private final JobLoggingListener jobLoggingListener;
    private final StepLoggingListener stepLoggingListener; // [1] StepLoggingListener 필드 추가

    public CustomerJpaCursorBatchConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            EntityManagerFactory entityManagerFactory,
            JpaCustomerRepository jpaCustomerRepository,
            ExternalApiSimulator externalApiSimulator,
            @Qualifier("batchTaskExecutor") AsyncTaskExecutor batchTaskExecutor,
            JobLoggingListener jobLoggingListener,
            StepLoggingListener stepLoggingListener) { // [2] 생성자 주입 추가

        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.entityManagerFactory = entityManagerFactory;
        this.jpaCustomerRepository = jpaCustomerRepository;
        this.externalApiSimulator = externalApiSimulator;
        this.batchTaskExecutor = batchTaskExecutor;
        this.jobLoggingListener = jobLoggingListener;
        this.stepLoggingListener = stepLoggingListener;
    }

    @Bean(name = "customerJpaMigrationJob")
    Job customerJpaMigrationJob() {
        return new JobBuilder("customerJpaMigrationJob", jobRepository)
                .listener(jobLoggingListener) // [로그백 분리용 Job 리스너]
                .start(customerJpaMigrationStep())
                .build();
    }

    // =========================================================================
    // [MULTITHREAD - 2] Step 멀티스레드 구성 방식
    // =========================================================================
    @Bean
    Step customerJpaMigrationStep() {
        return new StepBuilder("customerJpaMigrationStep", jobRepository)
                .<JpaCustomer, Future<JpaProcessedCustomer>>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerJpaCursorItemReader())
                .processor(asyncCustomerProcessor())
                .writer(asyncCustomerWriter())
                .listener(stepLoggingListener) // [3] 로그백 분리용 Step 리스너 등록
                .build();
    }

    @Bean
    @StepScope
    JpaCursorItemReader<JpaCustomer> customerJpaCursorItemReader() {
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
    ItemProcessor<JpaCustomer, JpaProcessedCustomer> customerProcessor() {
        return customer -> {
            log.debug("Processing JPA customer: {}", customer.getId());

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
    // [MULTITHREAD - 3] AsyncItemProcessor
    // =========================================================================
    @Bean
    AsyncItemProcessor<JpaCustomer, JpaProcessedCustomer> asyncCustomerProcessor() {
        AsyncItemProcessor<JpaCustomer, JpaProcessedCustomer> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(customerProcessor());
        asyncProcessor.setTaskExecutor(batchTaskExecutor);
        return asyncProcessor;
    }

    @Bean
    ItemWriter<JpaProcessedCustomer> customerItemWriter() {
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

            processedWriter.write(chunk);

            List<Long> customerIds = chunk.getItems().stream()
                    .map(item -> item.getId())
                    .toList();

            log.info("[JPA Chunk] Writing batch chunk size: {}", customerIds.size());

            jpaCustomerRepository.updateStatusForIds(customerIds, "PROCESSED");
        };
    }

    // =========================================================================
    // [MULTITHREAD - 4] AsyncItemWriter
    // =========================================================================
    @Bean
    AsyncItemWriter<JpaProcessedCustomer> asyncCustomerWriter() {
        AsyncItemWriter<JpaProcessedCustomer> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(customerItemWriter());
        return asyncWriter;
    }

    // Tasklet 예제 Job
    @Bean(name = "customerJpaMigrationTaskletJob")
    Job customerJpaMigrationTaskletJob() {
        return new JobBuilder("customerJpaMigrationTaskletJob", jobRepository)
                .listener(jobLoggingListener) // [로그백 분리용 Job 리스너]
                .start(customerJpaMigrationTaskletStep())
                .build();
    }

    @Bean
    Step customerJpaMigrationTaskletStep() {
        return new StepBuilder("customerJpaMigrationTaskletStep", jobRepository)
                .tasklet(customerJpaMigrationTasklet(), transactionManager)
                .listener(stepLoggingListener) // [4] Tasklet Step에도 로그백 분리용 Step 리스너 등록
                .build();
    }

    @Bean
    Tasklet customerJpaMigrationTasklet() {
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