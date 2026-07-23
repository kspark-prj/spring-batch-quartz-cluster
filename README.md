# Clustered Quartz & Spring Batch 5.x with Java 21 Virtual Threads

이 프로젝트는 Java 21, Spring Boot 3.x, Spring Batch 5.x, Quartz Scheduler (Cluster Mode), MyBatis, PostgreSQL을 활용하여 멀티 노드 환경에서 중복 실행 없이 안전하게 스케줄링을 관리하고, 대용량 처리를 고성능 병렬 방식으로 처리하는 백엔드 아키텍처 실무 예제입니다.

---

## 🛠️ 기술 스택 및 개발 환경
- **Language**: Java 21 (Virtual Threads 적극 활용)
- **Framework**: Spring Boot 3.2.4, Spring Batch 5.x
- **Scheduler**: Quartz Scheduler 2.x (Cluster Mode)
- **Persistence**: MyBatis 3.x, PostgreSQL
- **DB Tool**: Spring JDBC (`JdbcTemplate` 기반 고속 Bulk Insert)

---

## 📂 프로젝트 폴더 구조
```text
demo
├── pom.xml                                         # Maven 의존성 설정 파일
├── README.md                                       # 프로젝트 개발/실행 가이드 (본 파일)
├── Quartz-Collection.postman_collection.json       # Postman API 호출 테스트용 컬렉션 JSON
└── src
    └── main
        ├── java
        │   └── com
        │       └── example
        │           └── demo
        │               ├── DemoApplication.java     # 스프링 부트 메인 실행 클래스
        │               ├── batch
        │               │   ├── config
        │               │   │   └── CustomerBatchConfig.java   # Spring Batch Job 및 TaskExecutor 설정
        │               │   └── model
        │               │       ├── Customer.java              # 소스 테이블 Entity (Record)
        │               │       └── ProcessedCustomer.java     # 타겟 테이블 Entity (Record)
        │               ├── config
        │               │   ├── DatabaseConfig.java            # MyBatis 및 트랜잭션 설정
        │               │   ├── QuartzConfig.java              # Quartz 스케줄러 세부 바인딩 설정
        │               │   └── ThreadConfig.java              # Java 21 가상 스레드 executor 정의
        │               ├── dummy
        │               │   └── DummyDataGenerator.java        # 10만 건 고속 더미 데이터 생성기
        │               ├── mapper
        │               │   ├── CustomerMapper.java            # MyBatis Mapper 인터페이스
        │               │   └── ProcessedCustomerMapper.java   # MyBatis Mapper 인터페이스
        │               ├── quartz
        │               │   ├── controller
        │               │   │   └── QuartzJobController.java   # Quartz REST API 웹 컨트롤러
        │               │   ├── dto
        │               │   │   ├── JobRequest.java            # Job 등록 파라미터 DTO (Record)
        │               │   │   ├── JobResponse.java           # Job 상태 응답 DTO (Record)
        │               │   │   └── SchedulerStatusResponse.java # 스케줄러 통합 상태 DTO (Record)
        │               │   ├── job
        │               │   │   ├── SampleBatchTriggerJob.java # Batch를 구동시키는 Quartz Job
        │               │   │   └── SampleSystemMonitoringJob.java # 시스템 힙 메모리 모니터링 Job
        │               │   └── service
        │               │       └── QuartzJobService.java      # Quartz API 서비스 레이어
        │               │   └── util
        │               │       └── QuartzJobInitializer.java  # 애플리케이션 시작 시 기본 스케줄러 초기화 등록기
        │               └── support
        │                   └── ExternalApiSimulator.java      # 비동기 병렬 대기를 체감할 모의 REST API
        └── resources
            ├── application.yml                             # 서버 포트, DB, 가상 스레드, Quartz 클러스터링 설정
            ├── schema-postgresql.sql                       # PostgreSQL용 Quartz 및 비즈니스 테이블 DDL
            └── mapper
                ├── CustomerMapper.xml                      # Customer DB 조작 쿼리 XML
                └── ProcessedCustomerMapper.xml             # ProcessedCustomer DB 조작 쿼리 XML
```

---

## 🚀 빠른 시작 가이드 (Quick Start)

### 1. PostgreSQL DB 설정 및 스키마 초기화
- PostgreSQL 데이터베이스에 접속하여 `src/main/resources/schema-postgresql.sql` 파일의 전체 DDL을 실행하여 테이블들을 미리 생성합니다.
- `src/main/resources/application.yml`의 `spring.datasource` 내에 현재 구동 중인 DB 호스트, 포트, 패스워드를 올바르게 입력합니다.
  - *참고: 벌크 삽입 성능 향상을 위해 URL 뒤에 `rewriteBatchedStatements=true` 쿼리 파라미터가 추가되어 있습니다.*

### 2. 프로젝트 구동
- IDE(IntelliJ 등)에서 `DemoApplication.java`를 실행하거나 터미널에서 메이븐 명령어로 빌드 후 실행합니다:
  ```bash
  mvn spring-boot:run
  ```
- 구동 시 최초 1회에 한하여 `customer` 테이블에 10만 건의 PENDING 더미 데이터가 5000개 단위로 나누어 고속 벌크 삽입됩니다.
- 또한 `DefaultSystemMonitoringJob`(30초 주기), `DefaultBatchTriggerJob`(5분 주기)이 Quartz 스케줄 데이터베이스에 자동 등록되어 활성화됩니다.

---

## 🔬 핵심 모니터링 및 성능 테스트 방법

### 1. Quartz Cluster Mode (멀티 노드 분산 처리)
1. 두 개의 터미널을 열고 포트를 다르게 하여 프로세스를 각각 구동합니다.
   - **노드 1**: `java -jar target/demo-0.0.1-SNAPSHOT.jar --server.port=8080`
   - **노드 2**: `java -jar target/demo-0.0.1-SNAPSHOT.jar --server.port=8081`
2. 데이터베이스 테이블 `QRTZ_SCHEDULER_STATE`를 조회하면 두 개의 인스턴스가 15초 간격으로 상태를 체킹(Heartbeat)하는 것을 볼 수 있습니다.
3. 30초 주기로 실행되는 모니터링 Job은 단 하나의 노드 콘솔 로그에서만 출력되며, 두 노드에서 중복 동시 실행되지 않습니다.
4. 실행 노드를 강제 종료(Kill)하면, 대기하던 노드가 일정 시간 후 이를 감지하고 자동으로 실행 권한을 양도받아 Failover를 정상 처리합니다.

### 2. Java 21 Virtual Threads 성능 체감
- `ExternalApiSimulator` 클래스는 300~700ms의 네트워크 API 호출 지연을 가상으로 발생시킵니다.
- `CustomerBatchConfig`에서 **Virtual Thread Executor**를 지정한 `AsyncItemProcessor` 덕분에, 배치 처리 시 I/O 지연이 발생할 때마다 해당 스레드가 물리적으로 차단되지 않고 언마운트되어 수백 개 이상의 REST API 전송을 동시에 기다리게 됩니다.
- 기존의 스레드 풀 환경 대비 획기적으로 개선된 대용량 처리 시간을 콘솔 모니터링에서 직접 확인하실 수 있습니다.

---

## ✉️ RESTful API 컬렉션 테스팅
- 프로젝트 루트 디렉터리에 동봉된 `Quartz-Collection.postman_collection.json` 파일을 복사하여 Postman 웹 또는 데스크톱에서 **Import**한 뒤 즉시 API 관리 기능을 테스트하실 수 있습니다.
- 제공되는 API 명세:
  - `스케줄러 상태 및 전체 Job 목록 조회` (GET)
  - `동적 신규 Cron Job 추가` (POST)
  - `특정 Job 일시정지 (PAUSE)` (POST)
  - `정지된 Job 복구 (RESUME)` (POST)
  - `특정 Job 1회 즉시 실행 (Trigger)` (POST)
  - `특정 Job 영구 삭제` (DELETE)
