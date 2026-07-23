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
public class CustomerBatchConfig {

    private static final Logger log = LoggerFactory.getLogger(CustomerBatchConfig.class);
    private static final int CHUNK_SIZE = 1000;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final SqlSessionFactory sqlSessionFactory;
    private final CustomerMapper customerMapper;
    private final ProcessedCustomerMapper processedCustomerMapper;
    private final ExternalApiSimulator externalApiSimulator;
    private final AsyncTaskExecutor virtualThreadTaskExecutor;

    public CustomerBatchConfig(
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
    public Job customerMigrationJob() {
        return new JobBuilder("customerMigrationJob", jobRepository)
                .start(customerMigrationStep())
                .build();
    }

    @Bean
    public Step customerMigrationStep() {
        return new StepBuilder("customerMigrationStep", jobRepository)
                .<Customer, Future<ProcessedCustomer>>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerItemReader())
                .processor(asyncCustomerProcessor())
                .writer(asyncCustomerWriter())
                .build();
    }

    @Bean
    @StepScope
    public MyBatisPagingItemReader<Customer> customerItemReader() {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("status", "PENDING");

        return new MyBatisPagingItemReaderBuilder<Customer>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("com.example.demo.mapper.CustomerMapper.selectCustomersByStatus")
                .parameterValues(parameterValues)
                .pageSize(CHUNK_SIZE)
                .saveState(false)
                .build();
    }

    @Bean
    public ItemProcessor<Customer, ProcessedCustomer> customerProcessor() {
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

    @Bean
    public AsyncItemProcessor<Customer, ProcessedCustomer> asyncCustomerProcessor() {
        AsyncItemProcessor<Customer, ProcessedCustomer> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(customerProcessor());
        asyncProcessor.setTaskExecutor(virtualThreadTaskExecutor);
        return asyncProcessor;
    }

    @Bean
    public ItemWriter<ProcessedCustomer> customerItemWriter() {

        // =========================================================================
        // 1. Target 테이블(processed_customer) 저장용 MyBatisBatchItemWriter 생성
        // =========================================================================
        // - MyBatisBatchItemWriter는 BATCH ExecutorType 세션을 사용하여
        //   1,000건의 데이터를 단건 SQL(insertProcessedCustomer)에 addBatch()로 묶어 대량 Insert 합니다.
        MyBatisBatchItemWriter<ProcessedCustomer> processedWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.ProcessedCustomerMapper.insertProcessedCustomer")
                .assertUpdates(false) // Batch 실행 결과 개수 검증 비활성화 (Upsert 사용 시 필요)
                .build();

        // =========================================================================
        // 2. Source 테이블(customer) 상태 업데이트용 MyBatisBatchItemWriter 생성
        // =========================================================================
        // - [핵심] 일반 Mapper(customerMapper.updateStatuses)를 직접 부르면 SIMPLE 세션과 충돌(ExecutorType 예외)이 발생합니다.
        // - 이를 방지하기 위해 Status Update 역시 동일한 BATCH 모드의 Writer로 생성해 세션을 공유합니다.
        MyBatisBatchItemWriter<ProcessedCustomer> statusUpdateWriter = new MyBatisBatchItemWriterBuilder<ProcessedCustomer>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.example.demo.mapper.CustomerMapper.updateStatusSingle")
                .assertUpdates(false)
                .build();

        // =========================================================================
        // 3. Writer 필수 속성 검증 및 초기화
        // =========================================================================
        // - Spring Container가 직접 관리하지 않는 객체이므로 afterPropertiesSet()을 직접 호출하여
        //   sqlSessionFactory, statementId 등의 누락 여부를 실행 전 최종 검증합니다.
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
            // Chunk<ProcessedCustomer> 내부에서 실제 데이터 리스트(List) 추출
            List<? extends ProcessedCustomer> processedList = chunk.getItems();

            // 처리할 데이터가 없는 빈 청크인 경우 DB 호출 없이 조기 종료
            if (processedList.isEmpty()) {
                return;
            }

            log.info("Writing batch chunk size: {}", processedList.size());

            // [STEP 1] processed_customer 테이블에 대량 Insert / Upsert 실행
            // - 내부적으로 1,000개 데이터를 루프 돌며 PreparedStatement.addBatch() 후 executeBatch() 1회 호출
            processedWriter.write(chunk);

            // [STEP 2] 원본 customer 테이블의 status를 'PROCESSED'로 대량 Update 실행
            // - ProcessedCustomer 객체의 'id' 필드를 단건 Update 쿼리(updateStatusSingle)의 #{id}와 매핑
            // - STEP 1과 동일한 BATCH 세션을 사용하여 ExecutorType 충돌 없이 1회에 일괄 Update 수행
            statusUpdateWriter.write(chunk);
        };
    }

    @Bean
    public AsyncItemWriter<ProcessedCustomer> asyncCustomerWriter() {
        AsyncItemWriter<ProcessedCustomer> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(customerItemWriter());
        return asyncWriter;
    }
    // =========================================================================
    // Tasklet 예제 (한 번에 실행되는 배치)
    // =========================================================================

    @Bean(name = "customerMigrationTaskletJob")
    public Job customerMigrationTaskletJob() {
        return new JobBuilder("customerMigrationTaskletJob", jobRepository)
                .start(customerMigrationTaskletStep())
                .build();
    }

    @Bean
    public Step customerMigrationTaskletStep() {
        return new StepBuilder("customerMigrationTaskletStep", jobRepository)
                .tasklet(customerMigrationTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet customerMigrationTasklet() {
        return (contribution, chunkContext) -> {
            // 1. PENDING 상태의 전체 데이터 한 번에 조회
            List<Customer> pendingCustomers = customerMapper.selectCustomersByStatusPending();
            log.info("[Tasklet] Total pending customers fetched: {}", pendingCustomers.size());

            if (pendingCustomers.isEmpty()) {
                return RepeatStatus.FINISHED;
            }
            pendingCustomers.stream().forEach(s -> System.out.println(s.name()));

            log.info("[Tasklet] Successfully processed total {} items.", pendingCustomers.size());
            return RepeatStatus.FINISHED;
        };
    }
}
