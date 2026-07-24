---

--## 1. 종합 요약 (상태별 건수)
-- 현재 등록된 Trigger의 상태별 수량을 한눈에 확인합니다.

SELECT
    trigger_state,
    COUNT(*) AS count
FROM qrtz_triggers
GROUP BY trigger_state;

--## 2. 전체 스케줄 목록 및 실행 시간 조회
--등록된 Job과 Trigger의 연결 상태, Cron 표현식, **이전/다음 실행 시간**을 조회합니다.

SELECT
    j.job_name,
    j.job_group,
    j.job_class_name,
    t.trigger_name,
    t.trigger_type,
    t.trigger_state,
    -- 이전 실행 시간 (KST 기준)
    CASE WHEN t.prev_fire_time > 0
         THEN to_timestamp(t.prev_fire_time / 1000.0) AT TIME ZONE 'Asia/Seoul'
         ELSE NULL END AS last_fire_time,
    -- 다음 실행 시간 (KST 기준)
    CASE WHEN t.next_fire_time > 0
         THEN to_timestamp(t.next_fire_time / 1000.0) AT TIME ZONE 'Asia/Seoul'
         ELSE NULL END AS next_fire_time,
    c.cron_expression
FROM qrtz_job_details j
JOIN qrtz_triggers t ON j.sched_name = t.sched_name AND j.job_name = t.job_name AND j.job_group = t.job_group
LEFT JOIN qrtz_cron_triggers c ON t.sched_name = c.sched_name AND t.trigger_name = c.trigger_name AND t.trigger_group = c.trigger_group
ORDER BY t.next_fire_time ASC;


SELECT
    f.job_name,
    f.job_group,
    f.trigger_name,
    f.instance_name AS executed_by_instance, -- 실행 중인 서버 인스턴스명
    to_timestamp(f.fired_time / 1000.0) AT TIME ZONE 'Asia/Seoul' AS fired_time,
    f.state
FROM qrtz_fired_triggers f
WHERE f.state = 'EXECUTING';


--## 3. 현재 "실행 중"인 작업 (EXECUTING) 모니터링
--지금 수행 중인 배치 작업과 **수행된 지 몇 초/분 지났는지**를 실시간 모니터링합니다.
SELECT
    f.entry_id,
    f.instance_name,
    f.job_name,
    f.job_group,
    f.state,
    to_timestamp(f.fired_time / 1000.0) AT TIME ZONE 'Asia/Seoul' AS fired_time,
    -- 실행 후 경과 시간 (초 단위)
    ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - to_timestamp(f.fired_time / 1000.0)))) AS running_seconds
FROM qrtz_fired_triggers f
WHERE f.state = 'EXECUTING'
ORDER BY f.fired_time ASC;

--## 4. 장애 및 지연(Misfire) 작업 트러블슈팅
--에러가 발생했거나, 실행 시간이 지났는데도 멈춰있는 작업을 찾습니다.
-- 1) ERROR 또는 BLOCKED(이전 작업 누적으로 대기) 상태인 작업
SELECT
    job_name,
    trigger_name,
    trigger_state,
    to_timestamp(prev_fire_time / 1000.0) AT TIME ZONE 'Asia/Seoul' AS last_fire_time,
    to_timestamp(next_fire_time / 1000.0) AT TIME ZONE 'Asia/Seoul' AS next_fire_time
FROM qrtz_triggers
WHERE trigger_state IN ('ERROR', 'BLOCKED');

-- 2) 실행 예정 시간이 이미 지났는데도 아직 실행되지 못한(WAITING) 작업
SELECT
    job_name,
    trigger_name,
    trigger_state,
    to_timestamp(next_fire_time / 1000.0) AT TIME ZONE 'Asia/Seoul' AS should_have_fired_at
FROM qrtz_triggers
WHERE trigger_state = 'WAITING'
  AND next_fire_time < (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)
ORDER BY next_fire_time ASC;

--## 5. 클러스터(Cluster) 노드 헬스체크
--Quartz 다중 서버(노드) 환경에서 각 인스턴스가 DB에 Heartbeat를 제대로 남기고 있는지 확인합니다.
SELECT
    sched_name,
    instance_name,
    to_timestamp(last_checkin_time / 1000.0) AT TIME ZONE 'Asia/Seoul' AS last_checkin_time,
    checkin_interval,
    -- 마지막 체크인 이후 경과 시간 (초 단위)
    ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - to_timestamp(last_checkin_time / 1000.0)))) AS seconds_since_checkin
FROM qrtz_scheduler_state
ORDER BY last_checkin_time DESC;


-- 1) QRTZ_LOCKS 테이블의 기본 락 항목 확인
SELECT
    sched_name,
    lock_name
FROM qrtz_locks;

-- 2) 현재 락을 잡고 이벤트를 처리 중인 세션/트리거 상태 확인 (Fired Triggers와 연계)
SELECT
    f.sched_name,
    f.entry_id,
    f.instance_name AS locking_instance, -- 현재 락을 쥐고 작업을 수행 중인 노드
    f.trigger_name,
    f.job_name,
    f.state,
    to_timestamp(f.fired_time / 1000.0) AT TIME ZONE 'Asia/Seoul' AS fired_time
FROM qrtz_fired_triggers f
WHERE f.state IN ('ACQUIRED', 'EXECUTING');