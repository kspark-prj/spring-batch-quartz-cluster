package com.example.demo.config;

import org.springframework.boot.autoconfigure.quartz.QuartzProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Quartz 클러스터링 및 스프링 컨텍스트 빈 바인딩 설정 클래스입니다.
 */
@Configuration
public class QuartzConfig {

    private final ApplicationContext applicationContext;
    private final DataSource dataSource;
    private final PlatformTransactionManager transactionManager;
    private final QuartzProperties quartzProperties;

    public QuartzConfig(ApplicationContext applicationContext,
                        DataSource dataSource,
                        PlatformTransactionManager transactionManager,
                        QuartzProperties quartzProperties) {
        this.applicationContext = applicationContext;
        this.dataSource = dataSource;
        this.transactionManager = transactionManager;
        this.quartzProperties = quartzProperties;
    }

    @Bean
    public SpringBeanJobFactory springBeanJobFactory() {
        SpringBeanJobFactory jobFactory = new SpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        return jobFactory;
    }

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(SpringBeanJobFactory springBeanJobFactory) {
        SchedulerFactoryBean schedulerFactory = new SchedulerFactoryBean();
        schedulerFactory.setDataSource(dataSource);
        schedulerFactory.setTransactionManager(transactionManager);
        schedulerFactory.setJobFactory(springBeanJobFactory);
        
        Properties properties = new Properties();
        properties.putAll(quartzProperties.getProperties());
        schedulerFactory.setQuartzProperties(properties);
        
        schedulerFactory.setStartupDelay(5);
        schedulerFactory.setOverwriteExistingJobs(true);
        
        return schedulerFactory;
    }
}
