# Quartz 스케줄러 Job 등록 및 관리 가이드

이 문서에서는 본 프로젝트의 **Clustered Quartz Scheduler** 환경에서 새로운 Job을 작성하고 등록(정적/동적)하여 실행하는 방법에 대해 자세히 설명합니다.

---

## 📌 개요

본 프로젝트는 멀티 노드(Clustered) 환경에서 중복 실행 없이 안전하게 배치를 구동하기 위해 Quartz Scheduler를 활용합니다.
Quartz 관련 핵심 설정은 [QuartzConfig.java]에 정의되어 있으며, PostgreSQL 데이터베이스(`QRTZ_` 시작 테이블들)를 통해 Job 및 Trigger 상태를 관리합니다.

---

## 🛠️ Step 1. 새로운 Job 클래스 구현하기

Quartz Job을 등록하기 위해서는 먼저 `org.quartz.Job` 인터페이스를 구현하는 클래스를 작성해야 합니다.

### 1. Job 구현 예시

아래는 시스템 메모리 체크나 데이터 정리를 수행하는 가상의 `MyCustomJob` 구현체 예시입니다.

```java
package com.example.demo.quartz.job;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 사용자 정의 커스텀 Quartz Job 예시입니다.
 */
@Component // Spring Bean으로 등록하여 의존성 주입(DI)이 가능하게 합니다.
@DisallowConcurrentExecution // 동일한 JobDetail에 대해 다중 인스턴스가 동시에 실행되는 것을 방지합니다. (클러스터 필수 권장)
public class MyCustomJob implements org.quartz.Job {

    private static final Logger log = LoggerFactory.getLogger(MyCustomJob.class);

    // Spring Bean 주입 가능 (필요한 서비스나 리포지토리 선언)
    // private final MyService myService;
    // public MyCustomJob(MyService myService) { this.myService = myService; }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("=== MyCustomJob 실행 시작 ===");

        // 1. JobDataMap으로부터 파라미터 추출
        JobDataMap dataMap = context.getMergedJobDataMap();
        String actionType = dataMap.getString("actionType");
        int limit = dataMap.getIntValue("limit");

        log.info("파라미터 조회 - actionType: {}, limit: {}", actionType, limit);

        try {
            // 2. 비즈니스 로직 실행
            // myService.performAction(actionType, limit);
            log.info("비즈니스 로직이 정상 처리되었습니다.");

        } catch (Exception e) {
            log.error("Job 실행 중 에러 발생", e);
            // Job 실행 실패 시 Quartz에 예외를 던집니다.
            throw new JobExecutionException(e);
        }

        log.info("=== MyCustomJob 실행 완료 ===");
    }
}
```

### ⚠️ 구현 시 주의사항

1. **`@Component` 적용**: [QuartzConfig.java]의 `SpringBeanJobFactory` 덕분에 `@Component`가 선언된 Job 클래스에서는 생성자를 통해 스프링 컨텍스트의 빈들을 자유롭게 주입받을 수 있습니다.
2. **`@DisallowConcurrentExecution`**: 배치가 이중으로 실행되는 것을 막기 위해 클러스터 모드에서는 꼭 붙여주는 것이 좋습니다.
3. **`JobDataMap` 데이터 타입**: 동적으로 전달되는 파라미터는 데이터베이스에 직렬화되어 저장되므로, `JobDataMap` 내부 값들은 기본 타입(String, Integer 등)이나 Serializable 객체여야 합니다.

---

## ⚙️ Step 2. Job 등록하기 (방법 3가지)

Job을 스케줄러에 등록하여 실행시키는 방법은 크게 **정적 초기화 등록**, **REST API 동적 등록**, **자바 코드 프로그램적 등록**으로 나뉩니다.

---

### 방법 1. 애플리케이션 시작 시 자동 등록 (정적 초기화)

서버가 켜질 때 고정된 Cron 주기(예: 매일 새벽 2시 등)로 특정 Job이 항상 등록되어 있어야 하는 경우에 사용합니다.

- **대상 파일**: [QuartzJobInitializer.java]
- **등록 방식**: `CommandLineRunner`를 구현하여 애플리케이션 구동 시 DB에 해당 Job 정보가 없는 경우에만 최초 등록합니다.

#### 등록 예시 코드 추가하기

[QuartzJobInitializer.java]의 `run` 메서드 내부에 아래와 같이 코드를 추가합니다.

```java
// QuartzJobInitializer.java 내부

JobKey customKey = new JobKey("MyDefaultCustomJob", "CUSTOM_GROUP");
if (!scheduler.checkExists(customKey)) {
    JobRequest request = new JobRequest(
            "MyDefaultCustomJob",          // Job 이름
            "CUSTOM_GROUP",                // Job 그룹
            "0 0 2 * * ?",                 // Cron 표현식 (매일 새벽 2시)
            Map.of("actionType", "CLEAN", "limit", 100) // 전달할 파라미터
    );
    // 등록 요청 실행 (위에서 작성한 MyCustomJob.class 지정)
    quartzJobService.addJob(request, MyCustomJob.class);
    log.info("MyDefaultCustomJob이 정상적으로 초기 등록되었습니다.");
}
```

---

### 방법 2. REST API를 이용한 동적 등록

서버 재기동 없이 런타임에 새로운 스케줄을 추가하거나 변경하고 싶을 때 사용합니다.
본 프로젝트는 [QuartzJobController.java]를 통해 API를 노출하고 있습니다.

#### API 스펙

- **HTTP Method**: `POST`
- **URL**: `/api/quartz/jobs?type=BATCH` (또는 `type=MONITOR`)
    - _참고: 기본 제공 컨트롤러는 `type`에 따라 `SampleBatchTriggerJob` 또는 `SampleSystemMonitoringJob` 클래스를 선택하도록 매핑되어 있습니다. 새 커스텀 클래스를 API로 연결하려면 컨트롤러의 조건문을 수정하거나 신규 엔드포인트를 추가해야 합니다._
- **Content-Type**: `application/json`
- **Request Body ([JobRequest.java]**:

```json
{
    "jobName": "DynamicMigrationJob-1",
    "jobGroup": "MIGRATION_GROUP",
    "cronExpression": "0 0/10 * * * ?",
    "jobData": {
        "triggerReason": "Manual registration via API",
        "targetPartition": "202607"
    }
}
```

#### curl 요청 예시

```bash
curl -X POST "http://localhost:8080/api/quartz/jobs?type=BATCH" \
     -H "Content-Type: application/json" \
     -d "{\"jobName\":\"DynamicMigrationJob-1\",\"jobGroup\":\"MIGRATION_GROUP\",\"cronExpression\":\"0 0/10 * * * ?\",\"jobData\":{\"triggerReason\":\"API Test\"}}"
```

---

### 방법 3. 자바 코드로 동적 등록 (Programmatic)

서비스 내부 로직이나 이벤트 리스너 등에서 조건에 따라 즉시 스케줄러에 등록하는 방법입니다.

- **대상 서비스**: [QuartzJobService.java]
- **메서드**: `addJob(JobRequest request, Class<? extends Job> jobClass)`

#### 비즈니스 로직 내부 활용 예시

```java
@Autowired
private QuartzJobService quartzJobService;

public void scheduleUserTask(String userId, String cronExpr) {
    JobRequest request = new JobRequest(
        "UserTask-" + userId,
        "USER_JOBS",
        cronExpr,
        Map.of("userId", userId)
    );

    // 동적으로 스케줄 추가
    boolean result = quartzJobService.addJob(request, MyCustomJob.class);
    if (result) {
        log.info("사용자 {}에 대한 커스텀 스케줄 등록 완료", userId);
    } else {
        log.warn("이미 동일한 스케줄이 존재합니다.");
    }
}
```

---

## 🎮 스케줄러 제어 및 상태 확인 API 안내

등록된 Job들은 아래 제공되는 REST API들을 활용하여 일시정지, 재개, 즉시 1회 실행, 삭제를 자유롭게 제어할 수 있습니다.

### 1. 스케줄러 상태 및 전체 등록 Job 목록 조회

- **HTTP Method**: `GET`
- **URL**: `/api/quartz/status`
- **설명**: 현재 Quartz 인스턴스 정보, 클러스터 기동 상태 및 DB에 등록된 모든 Job 목록(현재 트리거 상태, Cron 표현식, 이전 실행 시각, 다음 실행 예정 시각 등)을 통합 조회합니다.

### 2. 특정 Job 일시 정지 (PAUSE)

- **HTTP Method**: `POST`
- **URL**: `/api/quartz/jobs/pause?name={Job이름}&group={Job그룹}`
- **설명**: 지정된 Job의 트리거 상태를 `PAUSED`로 변경하여 스케줄링 작동을 일시적으로 중단합니다.

### 3. 정지된 Job 복구 (RESUME)

- **HTTP Method**: `POST`
- **URL**: `/api/quartz/jobs/resume?name={Job이름}&group={Job그룹}`
- **설명**: `PAUSED` 상태의 Job을 다시 활성화하여 스케줄러에 의해 자동 실행될 수 있도록 복원합니다.

### 4. 특정 Job 즉시 1회 실행 (Trigger)

- **HTTP Method**: `POST`
- **URL**: `/api/quartz/jobs/trigger?name={Job이름}&group={Job그룹}`
- **Request Body (선택)**: `{"extraParamKey": "extraParamValue"}`
- **설명**: 기존 Cron 주기에 영향을 주지 않고, 현재 시점에 즉시 해당 Job을 딱 1회 강제 실행합니다. (파라미터를 추가하여 수동으로 재실행할 때 유용)

### 5. 특정 Job 영구 삭제 (Delete)

- **HTTP Method**: `DELETE`
- **URL**: `/api/quartz/jobs?name={Job이름}&group={Job그룹}`
- **설명**: 스케줄러와 데이터베이스에서 해당 Job 및 연관된 Trigger 정보를 영구히 제거합니다.

---

## 💡 Clustered Quartz 핵심 고려사항

1. **Misfire 처리 전략**
    - 서버 장애 등으로 인해 실행되어야 했을 시간(Cron Trigger Point)을 놓치고 나중에 서버가 켜졌을 때, 밀린 건들을 한꺼번에 실행하면 시스템 부하가 생길 수 있습니다.
    - 본 프로젝트는 `QuartzJobService.java`에서 `withMisfireHandlingInstructionDoNothing()` 옵션을 사용하여 **누락된 실행 건은 무시하고 다음 정규 스케줄 주기에 실행**하도록 안전하게 세팅되어 있습니다.
      withMisfireHandlingInstructionFireAndProceed
2. **Clustered 환경의 Idempotency (동등성/중복 실행 방지)**
    - 이중화된 서버 중 어떤 노드에서든 스케줄이 정상 발화하여 트랜잭션이 수행될 수 있습니다. 따라서 대상 비즈니스 로직(예: Spring Batch)은 동일 데이터에 대해 중복 처리되더라도 문제가 발생하지 않도록 **멱등성(Idempotency)**을 가지도록 설계해야 합니다.
