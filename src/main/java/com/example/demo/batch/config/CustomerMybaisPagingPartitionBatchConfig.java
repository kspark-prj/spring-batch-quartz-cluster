package com.example.demo.batch.config;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.demo.batch.model.Customer;
import com.example.demo.batch.model.ProcessedCustomer;
import com.example.demo.mapper.CustomerMapper;
import com.example.demo.support.ExternalApiSimulator;

/**
 * MyBatisPagingItemReader 기반의 Partitioning 적용 배치 설정 파일입니다.
 */
@Configuration
public class CustomerMybaisPagingPartitionBatchConfig {

    private static final Logger log = LoggerFactory.getLogger(CustomerMybaisPagingPartitionBatchConfig.class);
    private static final int CHUNK_SIZE = 1000;
    private static final int GRID_SIZE = 4; // 병렬로 실행할 파티션(슬레이브 스레드) 개수

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final SqlSessionFactory sqlSessionFactory;
    private final CustomerMapper customerMapper;
    private final ExternalApiSimulator externalApiSimulator;
    private final AsyncTaskExecutor virtualThreadTaskExecutor;

    public CustomerMybaisPagingPartitionBatchConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SqlSessionFactory sqlSessionFactory,
            CustomerMapper customerMapper,
            ExternalApiSimulator externalApiSimulator,
            @Qualifier("virtualThreadTaskExecutor") AsyncTaskExecutor virtualThreadTaskExecutor) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.sqlSessionFactory = sqlSessionFactory;
        this.customerMapper = customerMapper;
        this.externalApiSimulator = externalApiSimulator;
        this.virtualThreadTaskExecutor = virtualThreadTaskExecutor;
    }

    // =========================================================================
    // 1. Partitioning Job 설정
    // =========================================================================
    @Bean(name = "customerPartitionMigrationJob")
    public Job customerPartitionMigrationJob() {
        return new JobBuilder("customerPartitionMigrationJob", jobRepository)
                .start(partitionerStep()) // 마스터 Step 시작
                .build();
    }

    // =========================================================================
    // 2. Master Step 설정
    // - Partitioner를 이용해 데이터를 여러 구간으로 나누고,
    //   각 구간별 Worker Step(customerPartitionWorkerStep)을 병렬로 구동합니다.
    // =========================================================================
    @Bean
    public Step partitionerStep() {
        return new StepBuilder("partitionerStep", jobRepository)
                .partitioner("customerPartitionWorkerStep", customerPartitioner()) // 분할 기준 설정
                .step(customerPartitionWorkerStep())                                // 각 파티션이 실행할 Worker Step
                .gridSize(GRID_SIZE)                                               // 파티션 개수
                .taskExecutor(virtualThreadTaskExecutor)                            // 병렬 스레드 Executor
                .build();
    }

    // =========================================================================
    // 3. Partitioner 구현
    // - 전체 ID 범위를 조회하여 gridSize 개수만큼 minId ~ maxId 바운더리를 생성합니다.
    // =========================================================================
    @Bean
    public Partitioner customerPartitioner() {
        return gridSize -> {
            // ID 최소/최대값 조회 (CustomerMapper 추가 구현 필요)
            long minId = customerMapper.selectMinIdByStatus("PENDING");
            long maxId = customerMapper.selectMaxIdByStatus("PENDING");

            long targetSize = (maxId - minId) / gridSize + 1;

            Map<String, org.springframework.batch.item.ExecutionContext> result = new HashMap<>();
            long number = 0;
            long start = minId;
            long end = start + targetSize - 1;

            while (start <= maxId) {
                org.springframework.batch.item.ExecutionContext value = new org.springframework.batch.item.ExecutionContext();

                if (end >= maxId) {
                    end = maxId;
                }

                value.putLong("minId", start);
                value.putLong("maxId", end);
                result.put("partition" + number, value);

                start += targetSize;
                end += targetSize;
                number++;
            }

            log.info("[Partitioner] Total partitions created: {}", result.size());
            return result;
        };
    }

    // =========================================================================
    // 4. Worker (Slave) Step 설정
    // - 각 파티션 스레드에서 실제 데이터를 Read-Process-Write 하는 단위 Step입니다.
    // =========================================================================
    @Bean
    public Step customerPartitionWorkerStep() {
        return new StepBuilder("customerPartitionWorkerStep", jobRepository)
                .<Customer, ProcessedCustomer>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerPartitionItemReader(null, null)) // Dynamic 파라미터 바인딩
                .processor(customerPartitionProcessor())
                .writer(customerPartitionItemWriter())
                .build();
    }

    // =========================================================================
    // 5. Partitioned Reader
    // - @StepScope 및 #{stepExecutionContext['minId']} SpEL을 통해 각 파티션의 ID 영역만 조회합니다.
    // =========================================================================
    @Bean
    @StepScope
    public MyBatisPagingItemReader<Customer> customerPartitionItemReader(
            @Value("#{stepExecutionContext['minId']}") Long minId,
            @Value("#{stepExecutionContext['maxId']}") Long maxId) {

        log.info("[Worker Reader] Reading partition range: minId={}, maxId={}", minId, maxId);

        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("status", "PENDING");
        parameterValues.put("minId", minId);
        parameterValues.put("maxId", maxId);

        return new MyBatisPagingItemReaderBuilder<Customer>()
                .sqlSessionFactory(sqlSessionFactory)
                // MyBatis XML QueryId: minId <= id AND id <= maxId 범위 조건이 포함된 쿼리 필요
                .queryId("com.example.demo.mapper.CustomerMapper.selectCustomersByStatusAndIdRange")
                .parameterValues(parameterValues)
                .pageSize(CHUNK_SIZE)
                .saveState(false)
                .build();
    }

    // =========================================================================
    // 6. ItemProcessor
    // =========================================================================
    @Bean
    public ItemProcessor<Customer, ProcessedCustomer> customerPartitionProcessor() {
        return customer -> {
            log.debug("Processing customer ID: {}", customer.id());
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
    // 7. ItemWriter
    // =========================================================================
    @Bean
    public ItemWriter<ProcessedCustomer> customerPartitionItemWriter() {
        MyBatisBatchItemWriter<ProcessedCustomer> processedWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.ProcessedCustomerMapper.insertProcessedCustomer")
                .assertUpdates(false)
                .build();

        MyBatisBatchItemWriter<ProcessedCustomer> statusUpdateWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.CustomerMapper.updateStatusSingle")
                .assertUpdates(false)
                .build();

        try {
            processedWriter.afterPropertiesSet();
            statusUpdateWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("MyBatisBatchItemWriter 초기화 실패", e);
        }

        return chunk -> {
            List<? extends ProcessedCustomer> processedList = chunk.getItems();
            if (processedList.isEmpty()) {
                return;
            }

            log.info("[Worker Writer] Partition writing batch chunk size: {}", processedList.size());

            processedWriter.write(chunk);
            statusUpdateWriter.write(chunk);
        };
    }
}