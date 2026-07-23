package com.example.demo.quartz.job;
import com.example.demo.quartz.service.WebCrawlerService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 가상 스레드(Virtual Thread)를 활용하여 여러 URL을 병렬로 크롤링하는 Quartz Job 구현체입니다.
 */
@Component
@DisallowConcurrentExecution
public class ParallelCrawlJob implements org.quartz.Job {

    private static final Logger log = LoggerFactory.getLogger(ParallelCrawlJob.class);

    private final WebCrawlerService webCrawlerService;

    public ParallelCrawlJob(WebCrawlerService webCrawlerService) {
        this.webCrawlerService = webCrawlerService;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Parallel Crawl Job started. InstId: {}", context.getFireInstanceId());

        List<String> targetUrls = getTargetUrls(context);

        if (targetUrls == null || targetUrls.isEmpty()) {
            log.warn("Crawl target URL list is empty.");
            return;
        }

        // 작업 단위마다 가상 스레드를 생성하는 Executor 사용 (try-with-resources로 자동 종료 관리)
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            // 각 URL에 대해 가상 스레드 기반의 비동기 크롤링 작업 생성
            List<CompletableFuture<Void>> futures = targetUrls.stream()
                    .map(url -> CompletableFuture.runAsync(() -> {
                        try {
                            log.info("Start crawling [VirtualThread: {}]: {}", Thread.currentThread(), url);
                            webCrawlerService.crawl(url);
                            log.info("Finished crawling: {}", url);
                        } catch (Exception e) {
                            log.error("Failed to crawl URL: {}", url, e);
                        }
                    }, executor))
                    .toList();

            // 모든 병렬 크롤링 작업이 완료될 때까지 대기
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            log.info("All parallel crawling tasks completed successfully.");
        } catch (Exception e) {
            log.error("Error occurred during parallel crawl execution", e);
            throw new JobExecutionException("Parallel Crawl Job 실행 실패", e);
        }
    }

    private List<String> getTargetUrls(JobExecutionContext context) {
        JobDataMap dataMap = context.getMergedJobDataMap();

        if (dataMap.containsKey("targetUrls")) {
            return (List<String>) dataMap.get("targetUrls");
        }

        return List.of(
                "https://example.com/page1",
                "https://example.com/page2",
                "https://example.com/page3",
                "https://example.com/page4"
        );
    }
}
