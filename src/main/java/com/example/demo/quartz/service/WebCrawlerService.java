package com.example.demo.quartz.service;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WebCrawlerService {

    private static final Logger log = LoggerFactory.getLogger(WebCrawlerService.class);

    // HTTP 타임아웃 설정 (5초)
    private static final int TIMEOUT_MILLIS = 5000;

    // 차단 방지를 위한 기본 User-Agent
    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    /**
     * 지정된 URL의 웹 페이지를 크롤링하여 데이터를 수집합니다.
     *
     * @param url 크롤링 대상 URL
     */
    public void crawl(String url) {
        log.debug("Connecting to URL: {}", url);

        try {
            // 1. Jsoup을 사용한 HTTP GET 요청 및 HTML Document 수집
            Document doc = Jsoup.connect(url)
                    .userAgent(USER_AGENT)
                    .timeout(TIMEOUT_MILLIS)
                    .get();

            // 2. 원하는 데이터 파싱 (예: 페이지 제목, 특정 태그 요소 등)
            String title = doc.title();

            // 예시: h1 태그 내용 추출
            String mainHeader = doc.select("h1").text();

            log.info("[Crawl Success] URL: {} | Title: {} | Header: {}", url, title, mainHeader);

            // 3. 수집한 데이터를 DB에 저장하거나 후처리 로직 호출
            saveCrawledData(url, title, mainHeader);

        } catch (IOException e) {
            log.error("[Crawl Failed] HTTP 요청 또는 파싱 실패 - URL: {}, Error: {}", url, e.getMessage());
            // 필요한 경우 상위 Quartz Job으로 예외를 던지거나 재시도 로직을 구성할 수 있습니다.
            throw new RuntimeException("Crawling failed for URL: " + url, e);
        }
    }

    /**
     * 수집한 데이터를 DB 등에 저장하는 가상 메소드
     */
    private void saveCrawledData(String url, String title, String content) {
        // TODO: Repository를 이용한 DB 저장 로직 구현
        log.debug("Saving data to DB -> URL: {}", url);
    }
}
