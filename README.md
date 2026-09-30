# Clustered Quartz & Spring Batch 5.x with Java 21 Virtual Threads & Spring Security

이 프로젝트는 **Java 21 Virtual Threads**, **Spring Boot 3.x**, **Spring Security**, **Spring Batch 5.x**, **Quartz Scheduler (Cluster Mode)**, **MyBatis**, **Spring Data JPA**, **PostgreSQL**을 활용하여 멀티 노드 환경에서 중복 실행 없이 안전하게 스케줄링을 관리하고, 대용량 처리를 고성능 병렬 방식으로 처리하는 모니터링 대시보드 포함 백엔드 아키텍처 실무 예제입니다.

---

## 🛠️ 기술 스택 및 개발 환경

- **Language**: Java 21 (Virtual Threads 적극 활용)
- **Framework**: Spring Boot 3.2.4, Spring Security 6.x, Spring Batch 5.x, Spring Data JPA
- **Scheduler**: Quartz Scheduler 2.x (Cluster Mode)
- **Persistence**: MyBatis 3.x, Spring Data JPA, PostgreSQL
- **Frontend / Dashboard**: HTML5, Vue 3, Tailwind CSS (Glassmorphism), ECharts
- **DB Tool**: Spring JDBC (`JdbcTemplate` 기반 고속 Bulk Insert)

---

## 📂 프로젝트 폴더 구조

```text
spring-batch-quartz-cluster
├── pom.xml                                         # Maven 의존성 설정 파일
├── README.md                                       # 프로젝트 개발/실행 가이드 (본 파일)
├── batch-monitor.sql                               # Spring Batch 대시보드 쿼리 모음
├── quartz-monitor.sql                             # Quartz 스케줄러 대시보드 쿼리 모음
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
        │               │       ├── JpaCustomer.java           # JPA Customer 엔티티
        │               │       └── ProcessedCustomer.java     # 타겟 테이블 Entity (Record)
        │               ├── config
        │               │   ├── DatabaseConfig.java            # MyBatis 및 트랜잭션 설정
        │               │   ├── QuartzConfig.java              # Quartz 스케줄러 세부 바인딩 설정
        │               │   ├── ThreadConfig.java              # Java 21 가상 스레드 executor 정의
        │               │   └── WebMvcConfig.java              # 정적 뷰 컨트롤러 포워딩/리다이렉트 설정
        │               ├── dummy
        │               │   └── DummyDataGenerator.java        # 10만 건 고속 더미 데이터 생성기
        │               ├── mapper
        │               │   ├── CustomerMapper.java            # MyBatis Mapper 인터페이스
        │               │   └── ProcessedCustomerMapper.java   # MyBatis Mapper 인터페이스
        │               ├── quartz
        │               │   ├── controller
        │               │   │   └── QuartzJobController.java   # Quartz REST API 웹 컨트롤러
        │               │   ├── dto
        │               │   │   ├── JobHistoryResponse.java    # 실행 이력 응답 DTO
        │               │   │   ├── JobRequest.java            # Job 등록 파라미터 DTO (Record)
        │               │   │   ├── JobResponse.java           # Job 상태 응답 DTO (Record)
        │               │   │   └── SchedulerStatusResponse.java # 스케줄러 통합 상태 DTO (Record)
        │               │   ├── job
        │               │   │   ├── CustomerMigrationQuartzJob.java # 고객 마이그레이션 실행 Job
        │               │   │   ├── ParallelCrawlJob.java      # 병렬 크롤링 작업 Job
        │               │   │   ├── SampleBatchTriggerJob.java # Batch를 구동시키는 Quartz Job
        │               │   │   └── SampleSystemMonitoringJob.java # 시스템 힙 메모리 모니터링 Job
        │               │   └── service
        │               │       ├── QuartzJobService.java      # Quartz API 서비스 레이어
        │               │       └── WebCrawlerService.java     # 웹 크롤링 서비스
        │               ├── security
        │               │   ├── config
        │               │   │   └── SecurityConfig.java        # Spring Security 6.x 보안 설정
        │               │   ├── entity
        │               │   │   └── User.java                  # 사용자 보안 JPA 엔티티 (users 테이블)
        │               │   ├── init
        │               │   │   └── DataInitializer.java       # 관리자 계정(admin) 자동검증/생성기
        │               │   ├── repository
        │               │   │   └── UserRepository.java        # 사용자 JPA 레포지토리
        │               │   └── service
        │               │       └── CustomUserDetailsService.java # UserDetailsService 구현체
        │               └── support
        │                   └── ExternalApiSimulator.java      # 비동기 병렬 대기를 체감할 모의 REST API
        └── resources
            ├── application.yml                             # DB, 가상 스레드, Quartz, JPA 설정
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

## 🔒 보안 및 인증 (Spring Security)

- **보안 접근 제어**: `http://localhost:8080/` 및 `/dashboard/index.html` 접속 시 미인증 사용자는 자동으로 모던 로그인 화면(`login.html`)으로 이동합니다.
- **비밀번호 암호화**: `BCryptPasswordEncoder` 적용
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

## 🔬 핵심 테스트 가이드

### 1. Quartz Cluster Mode (멀티 노드 분산 처리)

1. 두 개의 터미널에서 서로 다른 포트로 어플리케이션을 각각 구동합니다.
   ```bash
   java -jar target/demo-0.0.1-SNAPSHOT.jar --server.port=8080
   java -jar target/demo-0.0.1-SNAPSHOT.jar --server.port=8081
   ```
2. DB의 `QRTZ_SCHEDULER_STATE` 테이블 조회 시 두 개의 인스턴스가 15초 간격으로 핑(Heartbeat)을 주고받는 것을 확인합니다.
3. 실행 노드가 중단(Kill)되면 대기 노드가 감지하여 자동으로 권한을 승계(Failover)합니다.

### 2. Java 21 Virtual Threads 성능 체감

- `CustomerBatchConfig`에서 **Virtual Thread Executor**를 적용한 `AsyncItemProcessor`를 통해 I/O 대기 시간 동안 물리 스레드가 차단되지 않고 언마운트되어 수백 개 이상의 REST API 대기를 효율적으로 처리합니다.

---

## ✉️ RESTful API (Postman Collection)

프로젝트 루트의 `Quartz-Collection.postman_collection.json`을 Postman에 Import하여 API 관리 기능을 직접 테스트할 수 있습니다.
