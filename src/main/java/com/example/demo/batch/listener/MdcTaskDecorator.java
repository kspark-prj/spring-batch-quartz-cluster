package com.example.demo.batch.listener;

import java.util.Map;

import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;

/**
 * 메인/부모 스레드(Job/Step)의 MDC 컨텍스트를
 * 비동기 배치 작업 스레드(batch-exec-*)로 전파하고,
 * 작업 완료 후 스레드 풀 재사용에 따른 컨텍스트 오염(Leak)을 방지하는 Decorator입니다.
 */
public class MdcTaskDecorator implements TaskDecorator {

    @Override
    public Runnable decorate(Runnable runnable) {
        // [1] 호출 스레드(부모/메인)의 MDC 컨텍스트 스냅샷 복사
        Map<String, String> contextMap = MDC.getCopyOfContextMap();

        return () -> {
            try {
                // [2] 작업 스레드 실행 전 MDC 바인딩
                if (contextMap != null) {
                    MDC.setContextMap(contextMap);
                } else {
                    MDC.clear();
                }
                runnable.run();
            } finally {
                // [3] 작업 완료 후 스레드 풀 반환 전 MDC 정리 (스레드 재사용 시 컨텍스트 오염 방지)
                MDC.clear();
            }
        };
    }
}