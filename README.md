# Clustered Quartz & Spring Batch 5.x with Java 21 Virtual Threads & Spring Security

이 프로젝트는 **Java 21**, **Spring Boot 3.2.4**, **Spring Security 6.x**, **Spring Batch 5.x**, **Quartz Scheduler (Cluster Mode)**, **MyBatis 3.x**, **Spring Data JPA**, **PostgreSQL**을 활용하여 멀티 노드 환경에서 중복 실행 없이 안전하게 스케줄링을 관리하고, 대용량 처리를 고성능 병렬 방식으로 처리하는 모니터링 대시보드 포함 백엔드 아키텍처 실무 예제입니다.

---

## 🛠️ 기술 스택 및 개발 환경

| 구분 | 기술 |
|------|------|
| **Language** | Java 21 (Virtual Threads 활용) |
| **Framework** | Spring Boot 3.2.4, Spring Security 6.x, Spring Batch 5.x, Spring Data JPA |
| **Scheduler** | Quartz Scheduler 2.x (JDBC Cluster Mode) |
| **Persistence** | MyBatis 3.0.3 (`mybatis-spring-boot-starter`), Spring Data JPA, PostgreSQL |
| **Web Crawling** | Jsoup 1.17.2 |
| **Frontend / Dashboard** | HTML5, Vue 3, Tailwind CSS (Glassmorphism), ECharts |
| **Logging** | Logback SiftingAppender + MDC (Job별 로그 파일 자동 분리) |
| **Build** | Maven, Lombok, jspecify 1.0.0 |
| **DB Tool** | Spring JDBC (`JdbcTemplate` 기반 고속 Bulk Insert) |

---

## 📂 프로젝트 폴더 구조

```text
spring-batch-quartz-cluster
├── pom.xml                                         # Maven 의존성 설정 파일
├── README.md                                       # 프로젝트 개발/실행 가이드 (본 파일)
├── batch-monitor.sql                               # Spring Batch 대시보드 쿼리 모음
├── quartz-monitor.sql                              # Quartz 스케줄러 대시보드 쿼리 모음
├── Quartz-Collection.postman_collection.json       # Postman API 호출 테스트용 컬렉션 JSON
└── src
    └── main
        ├── java
        │   └── com
        │       └── example
        │           └── demo
        │               ├── DemoApplication.java                # 스프링 부트 메인 실행 클래스
        │               ├── batch
        │               │   ├── config
        │               │   │   ├── CustomerJpaCursorBatchConfig.java        # JPA Cursor 기반 Batch Job 설정
        │               │   │   ├── CustomerJpaPagingBatchConfig.java        # JPA Paging 기반 Batch Job 설정
        │               │   │   ├── CustomerMybaisPagingPartitionBatchConfig.java # MyBatis Paging + Partitioning 설정
        │               │   │   ├── CustomerMybatisCursorBatchConfig.java    # MyBatis Cursor 기반 Batch Job 설정
        │               │   │   └── CustomerMybatisPagingBatchConfig.java    # MyBatis Paging 기반 Batch Job 설정
        │               │   ├── listener
        │               │   │   ├── JobLoggingListener.java      # Job 실행 전후 로그 출력 리스너
        │               │   │   ├── MdcTaskDecorator.java        # 비동기 스레드 MDC 컨텍스트 전파 Decorator
        │               │   │   └── StepLoggingListener.java     # Step 실행 전후 MDC·로그 관리 리스너
        │               │   └── model
        │               │       ├── Customer.java                # 소스 테이블 MyBatis Record
        │               │       ├── JpaCustomer.java             # JPA Customer 엔티티 (customer 테이블)
        │               │       ├── JpaProcessedCustomer.java    # JPA ProcessedCustomer 엔티티 (processed_customer 테이블)
        │               │       └── ProcessedCustomer.java       # 타겟 테이블 MyBatis Record
        │               ├── config
        │               │   ├── DatabaseConfig.java              # MyBatis SqlSessionFactory 및 트랜잭션 설정
        │               │   ├── QuartzConfig.java                # Quartz 클러스터링 및 Spring Bean 바인딩 설정
        │               │   ├── ThreadConfig.java                # Batch 전용 ThreadPoolTaskExecutor 빈 설정
        │               │   ├── WebConfig.java                   # CORS 정책 및 정적 리소스 핸들러 설정
        │               │   └── WebMvcConfig.java                # 뷰 컨트롤러 포워딩/리다이렉트 설정
        │               ├── dummy
        │               │   └── DummyDataGenerator.java          # 10만 건 고속 더미 데이터 생성기
        │               ├── mapper
        │               │   ├── CustomerMapper.java              # MyBatis Mapper (페이징/커서/파티션 쿼리)
        │               │   └── ProcessedCustomerMapper.java     # MyBatis Mapper (처리 완료 데이터 Insert)
        │               ├── quartz
        │               │   ├── controller
        │               │   │   └── QuartzJobController.java     # Quartz REST API 웹 컨트롤러
        │               │   ├── dto
        │               │   │   ├── JobHistoryResponse.java      # 실행 이력 응답 DTO (Record)
        │               │   │   ├── JobRequest.java              # Job 등록 파라미터 DTO (Record)
        │               │   │   ├── JobResponse.java             # Job 상태 응답 DTO (Record)
        │               │   │   └── SchedulerStatusResponse.java # 스케줄러 통합 상태 DTO (Record)
        │               │   ├── job
        │               │   │   ├── CustomerMigrationQuartzJob.java  # 고객 마이그레이션 Batch 실행 Quartz Job
        │               │   │   ├── ParallelCrawlJob.java            # Virtual Thread 병렬 웹 크롤링 Job
        │               │   │   ├── SampleBatchTriggerJob.java       # Spring Batch를 구동시키는 Quartz Job
        │               │   │   └── SampleSystemMonitoringJob.java   # 시스템 힙 메모리 모니터링 Job
        │               │   ├── listener
        │               │   │   └── QuartzJobMdcListener.java    # Quartz Job 글로벌 MDC 자동 관리 리스너
        │               │   ├── service
        │               │   │   ├── QuartzJobService.java        # Quartz API 서비스 레이어
        │               │   │   └── WebCrawlerService.java       # Jsoup 기반 웹 크롤링 서비스
        │               │   └── util
        │               │       └── QuartzJobInitializer.java    # 구동 시 기본 Job 자동 등록 이니셜라이저
        │               ├── repository
        │               │   ├── JpaCustomerRepository.java       # JPA Customer 레포지토리
        │               │   └── JpaProcessedCustomerRepository.java # JPA ProcessedCustomer 레포지토리
        │               ├── security
        │               │   ├── config
        │               │   │   └── SecurityConfig.java          # Spring Security 6.x 보안 설정
        │               │   ├── entity
        │               │   │   └── User.java                    # 사용자 보안 JPA 엔티티 (users 테이블)
        │               │   ├── init
        │               │   │   └── DataInitializer.java         # 관리자 계정(admin) 자동검증/생성기
        │               │   ├── repository
        │               │   │   └── UserRepository.java          # 사용자 JPA 레포지토리
        │               │   └── service
        │               │       └── CustomUserDetailsService.java # UserDetailsService 구현체
        │               └── support
        │                   └── ExternalApiSimulator.java         # 비동기 병렬 대기를 체감할 모의 REST API
        └── resources
            ├── application.yml                             # DB, Quartz, JPA, Batch, MyBatis, Logging 설정
            ├── logback-spring.xml                          # MDC 기반 Job별 로그 파일 분리 설정
            ├── schema-postgresql.sql                       # Quartz, Batch, Security, 비즈니스 전체 DDL/DML
            ├── user-schema.sql                             # Security users 테이블 전용 DDL/DML
            ├── mapper
            │   ├── CustomerMapper.xml                      # Customer DB 조작 쿼리 XML
            │   └── ProcessedCustomerMapper.xml             # ProcessedCustomer DB 조작 쿼리 XML
            └── static
                ├── login.html                              # 모던 Glassmorphism 로그인 페이지
                └── dashboard
                    └── index.html                          # Vue 3 + Tailwind + ECharts 모니터링 대시보드
```

---

## 🔄 Spring Batch Job 구성 전략

프로젝트에는 **5가지 Reader/Writer 조합**의 Batch Job이 구현되어 있어 데이터 접근 전략에 따라 선택 사용할 수 있습니다.

| Config 클래스 | Reader 방식 | Writer 방식 | 비고 |
|---|---|---|---|
| `CustomerJpaCursorBatchConfig` | JPA Cursor (`JpaCursorItemReader`) | JPA (`JpaItemWriter`) | 대용량 스트리밍 처리에 적합 |
| `CustomerJpaPagingBatchConfig` | JPA Paging (`JpaPagingItemReader`) | JPA (`JpaItemWriter`) | 페이지 단위 처리 |
| `CustomerMybatisCursorBatchConfig` | MyBatis Cursor (`MyBatisCursorItemReader`) | MyBatis Batch (`MyBatisBatchItemWriter`) | MyBatis SQL 기반 스트리밍 |
| `CustomerMybatisPagingBatchConfig` | MyBatis Paging (`MyBatisPagingItemReader`) | MyBatis Batch (`MyBatisBatchItemWriter`) | MyBatis SQL 기반 페이징 |
| `CustomerMybaisPagingPartitionBatchConfig` | MyBatis Paging + **Partitioning** | MyBatis Batch (`MyBatisBatchItemWriter`) | PK 범위 기반 병렬 파티셔닝 |

- 각 Config는 `AsyncItemProcessor` + `AsyncItemWriter`를 활용하여 **비동기 병렬 처리**를 지원합니다.
- `ExternalApiSimulator`를 통해 외부 API 호출 지연을 시뮬레이션하며, I/O 대기 시간 동안 스레드가 차단되지 않도록 설계되었습니다.
- `JobLoggingListener`, `StepLoggingListener`가 모든 Job/Step 실행 전후 로그를 자동 기록합니다.

---

## 📊 로깅 아키텍처 (MDC + SiftingAppender)

프로젝트는 **Job별 로그 파일 자동 분리**를 구현하고 있습니다.

```
logs/
├── default/default.log                    # Job 외부의 일반 로그
├── DefaultSystemMonitoringJob/            # 시스템 모니터링 Job 전용 로그
│   ├── DefaultSystemMonitoringJob.log
│   └── archived/
├── DefaultBatchTriggerJob/                # 배치 트리거 Job 전용 로그
│   ├── DefaultBatchTriggerJob.log
│   └── archived/
└── CustomerMigrationTaskletJob/           # 고객 마이그레이션 Job 전용 로그
    ├── CustomerMigrationTaskletJob.log
    └── archived/
```

- **MDC 전파 체인**: `QuartzJobMdcListener` → `JobLoggingListener` → `StepLoggingListener` → `MdcTaskDecorator`
- **SiftingAppender**: `logback-spring.xml`에서 MDC의 `jobName` 키를 기준으로 로그 파일을 동적 분리
- **로그 보관 정책**: 일별 롤링, 단일 파일 100MB, 30일 보관, 총 용량 1GB 제한

---

## 🔒 보안 및 인증 (Spring Security)

- **보안 접근 제어**: `http://localhost:8080/` 및 `/dashboard/index.html` 접속 시 미인증 사용자는 자동으로 모던 로그인 화면(`login.html`)으로 이동합니다.
- **비밀번호 암호화**: `BCryptPasswordEncoder` 적용
- **CSRF**: 비활성화 (REST API 호환)
- **초기 관리자 계정 정보**:
  - **아이디**: `admin`
  - **비밀번호**: `admin1!`
  - *애플리케이션 구동 시 `DataInitializer`가 DB를 자동 검사하여 계정이 없거나 비밀번호 해시가 일치하지 않을 경우 `admin1!`의 BCrypt 해시로 자동 생성 및 업데이트합니다.*

---

## 🚀 빠른 시작 가이드 (Quick Start)

### 1. PostgreSQL DB 설정 및 스키마 초기화

1. PostgreSQL 데이터베이스를 구동하고 `src/main/resources/schema-postgresql.sql` (또는 `user-schema.sql`) DDL을 실행합니다.
2. `src/main/resources/application.yml`의 `spring.datasource` 내에 DB 접속 정보(Host, Port, Username, Password)를 설정합니다.

### 2. 프로젝트 구동

IDE에서 `DemoApplication.java`를 실행하거나 터미널에서 메이븐으로 구동합니다:

```bash
mvn spring-boot:run
```

구동 후 웹 브라우저에서 접속합니다:
- **접속 주소**: `http://localhost:8080` (자동으로 `/login.html`로 이동)
- **로그인 계정**: `admin` / `admin1!`

### 3. 자동 초기화 항목

애플리케이션 구동 시 다음 항목이 자동으로 실행됩니다:

| 순서 | 컴포넌트 | 동작 |
|------|----------|------|
| 1 | `DataInitializer` | 관리자 계정(admin) 존재 여부 확인 및 자동 생성/업데이트 |
| 2 | `DummyDataGenerator` | customer 테이블에 데이터가 없으면 10만 건 더미 데이터 고속 생성 |
| 3 | `QuartzJobInitializer` | 기본 Quartz Job 3종 자동 등록 (아래 참고) |

**자동 등록되는 Quartz Job:**

| Job 이름 | 그룹 | Cron | 설명 |
|----------|------|------|------|
| `DefaultSystemMonitoringJob` | `MONITOR_GROUP` | `0/30 * * * * ?` | 30초마다 힙 메모리 모니터링 |
| `DefaultBatchTriggerJob` | `BATCH_GROUP` | `0 0/5 * * * ?` | 5분마다 Spring Batch Job 실행 |
| `CustomerMigrationTaskletJob` | `BATCH_GROUP` | `0 0/2 * * * ?` | 2분마다 고객 마이그레이션 Batch 실행 |

---

## 🖥️ 주요 화면 및 기능

### 1. 모던 로그인 화면 (`/login.html`)
- Glassmorphism dark-mode 기반 프리미엄 카드 디자인
- 비밀번호 보기/숨기기 토글 지원
- 로그인 실패 및 로그아웃 성공 실시간 알림 뱃지

### 2. Quartz Scheduler 대시보드 (`/dashboard/index.html`)
- **실시간 요약 카드**: Total, Active, Paused, Error 상태 시각화
- **ECharts 히트맵**: Apache Airflow Grid View 스타일 실행 상태 매트릭스
- **Job 제어 기능**: 
  - 신규 Cron Job 추가
  - 특정 Job 즉시 실행 (Trigger), 일시 정지 (Pause), 복구 (Resume), 스케줄 변경 (Reschedule), 삭제 (Delete)
- **자동 새로고침 및 로그아웃**: 5초/10초/30초 설정 및 안전한 세션 종료 로그아웃 지원

---

## 📡 RESTful API 엔드포인트

모든 API는 `/api/quartz` 경로 하위에 위치하며, 인증된 사용자만 접근 가능합니다.

| Method | Endpoint | 설명 | 주요 파라미터 |
|--------|----------|------|---------------|
| `GET` | `/api/quartz/status` | 스케줄러 상태 및 등록 Job 목록 조회 | - |
| `GET` | `/api/quartz/jobs` | 등록된 모든 Job 상태 조회 | - |
| `GET` | `/api/quartz/history` | Job 실행 이력 조회 | `limit` (기본값 100) |
| `POST` | `/api/quartz/jobs` | 신규 Cron Job 동적 등록 | Body: `JobRequest`, Query: `jobBeanName` |
| `DELETE` | `/api/quartz/jobs` | 특정 Job 삭제 | `name`, `group` |
| `PUT` | `/api/quartz/jobs/cron` | 크론 표현식 변경 | `name`, `group`, `cronExpression` |
| `POST` | `/api/quartz/jobs/pause` | 특정 Job 일시 중단 | `name`, `group` |
| `POST` | `/api/quartz/jobs/resume` | 특정 Job 활성화 복구 | `name`, `group` |
| `POST` | `/api/quartz/jobs/trigger` | 특정 Job 즉시 1회 수동 실행 | `name`, `group`, Body(선택): 추가 파라미터 |

> 📌 프로젝트 루트의 `Quartz-Collection.postman_collection.json`을 Postman에 Import하여 위 API를 직접 테스트할 수 있습니다.

---

## 🔬 핵심 테스트 가이드

### 1. Quartz Cluster Mode (멀티 노드 분산 처리)

1. 두 개의 터미널에서 서로 다른 포트로 애플리케이션을 각각 구동합니다.
   ```bash
   java -jar target/spring-batch-quartz-cluster-0.0.1-SNAPSHOT.jar --server.port=8080
   java -jar target/spring-batch-quartz-cluster-0.0.1-SNAPSHOT.jar --server.port=8081
   ```
2. DB의 `QRTZ_SCHEDULER_STATE` 테이블 조회 시 두 개의 인스턴스가 15초 간격으로 핑(Heartbeat)을 주고받는 것을 확인합니다.
3. 실행 노드가 중단(Kill)되면 대기 노드가 감지하여 자동으로 권한을 승계(Failover)합니다.

### 2. Batch Job Reader 전략별 성능 비교

다양한 Reader 전략을 비교 테스트할 수 있습니다:
- **JPA Cursor vs JPA Paging**: 대용량 데이터 스트리밍 vs 페이지 단위 처리
- **MyBatis Cursor vs MyBatis Paging**: 네이티브 SQL 기반의 스트리밍/페이징 비교
- **MyBatis Paging + Partitioning**: PK 범위 기반 병렬 파티셔닝으로 처리 속도 극대화

### 3. 비동기 병렬 처리 성능 체감

- 각 Batch Config에서 `AsyncItemProcessor` + `batchTaskExecutor`(ThreadPoolTaskExecutor)를 통해 `ExternalApiSimulator`의 I/O 대기를 병렬화합니다.
- `MdcTaskDecorator`가 비동기 스레드에도 MDC 컨텍스트를 정확히 전파하여 Job별 로그 분리가 유지됩니다.

### 4. 병렬 웹 크롤링 (Virtual Thread)

- `ParallelCrawlJob`이 `Executors.newVirtualThreadPerTaskExecutor()`를 활용하여 여러 URL을 동시에 크롤링합니다.
- `WebCrawlerService`가 Jsoup으로 HTML을 파싱하고 데이터를 수집합니다.

---

## 📁 SQL 모니터링 쿼리

| 파일 | 용도 |
|------|------|
| `batch-monitor.sql` | Spring Batch 메타 테이블 조회 쿼리 모음 |
| `quartz-monitor.sql` | Quartz 스케줄러 메타 테이블 조회 쿼리 모음 |
