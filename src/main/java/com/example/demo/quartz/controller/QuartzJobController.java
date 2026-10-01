package com.example.demo.quartz.controller;

import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.quartz.dto.JobHistoryResponse;
import com.example.demo.quartz.dto.JobRequest;
import com.example.demo.quartz.dto.SchedulerStatusResponse;
import com.example.demo.quartz.service.QuartzJobService;

/**
 * Quartz 스케줄러 동적 관리를 위한 REST Controller입니다.
 */
@RestController
@RequestMapping("/api/quartz")
public class QuartzJobController {

    private final QuartzJobService quartzJobService;
    private final ApplicationContext applicationContext;

    public QuartzJobController(QuartzJobService quartzJobService,ApplicationContext applicationContext) {
        this.quartzJobService = quartzJobService;
        this.applicationContext = applicationContext;
    }

    @GetMapping("/status")
    public ResponseEntity<SchedulerStatusResponse> getStatus() {
        return ResponseEntity.ok(quartzJobService.getSchedulerStatus());
    }

    /**
     * 등록된 모든 Job의 목록 및 상태를 조회합니다.
     * 프론트엔드 대시보드의 Job 테이블 및 요약 카드에 사용됩니다.
     */
    @GetMapping("/jobs")
    public ResponseEntity<SchedulerStatusResponse> getJobs() {
        return ResponseEntity.ok(quartzJobService.getSchedulerStatus());
    }

    /**
     * Job 실행 이력을 조회합니다.
     * Airflow 스타일 그리드/히트맵 시각화에 사용됩니다.
     */
    @GetMapping("/history")
    public ResponseEntity<List<JobHistoryResponse>> getHistory(
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        return ResponseEntity.ok(quartzJobService.getJobHistory(limit));
    }

    @PostMapping("/jobs")
    public ResponseEntity<String> createJob(
            @RequestBody JobRequest request,
            @RequestParam(value = "jobBeanName") String jobBeanName) { // 예: "sampleBatchTriggerJob"

        // 1. Spring Context에서 빈 이름으로 Class 타입 조회
        Class<?> beanType = applicationContext.getType(jobBeanName);

        if (beanType == null || !org.quartz.Job.class.isAssignableFrom(beanType)) {
            return ResponseEntity.badRequest().body("존재하지 않거나 Quartz Job이 아닌 Bean입니다.");
        }

        @SuppressWarnings("unchecked")
        Class<? extends org.quartz.Job> targetJobClass = (Class<? extends org.quartz.Job>) beanType;

        // 2. Quartz에 등록
        boolean created = quartzJobService.addJob(request, targetJobClass);
        return created ? ResponseEntity.ok("성공") : ResponseEntity.badRequest().body("중복");
    }

    @DeleteMapping("/jobs")
    public ResponseEntity<String> deleteJob(
            @RequestParam("name") String name,
            @RequestParam("group") String group) {

        boolean deleted = quartzJobService.deleteJob(name, group);
        if (deleted) {
            return ResponseEntity.ok("스케줄 Job이 삭제되었습니다.");
        } else {
            return ResponseEntity.status(404).body("삭제하려는 Job 정보를 찾을 수 없습니다.");
        }
    }
    /**
     * 특정 Job의 크론 표현식(Cron Expression)을 변경합니다.
     */
    @PutMapping("/jobs/cron")
    public ResponseEntity<String> updateJobCron(
            @RequestParam("name") String name,
            @RequestParam("group") String group,
            @RequestParam("cronExpression") String cronExpression) {

        boolean updated = quartzJobService.updateCronExpression(name, group, cronExpression);
        if (updated) {
            return ResponseEntity.ok("스케줄 Job의 크론 표현식이 성공적으로 변경되었습니다.");
        } else {
            return ResponseEntity.status(404).body("크론 표현식을 변경할 대상 Trigger/Job을 찾을 수 없습니다.");
        }
    }

    @PostMapping("/jobs/pause")
    public ResponseEntity<String> pauseJob(
            @RequestParam("name") String name,
            @RequestParam("group") String group) {

        quartzJobService.pauseJob(name, group);
        return ResponseEntity.ok("선택한 Job 스케줄이 정상적으로 일시 중단(PAUSE)되었습니다.");
    }

    @PostMapping("/jobs/resume")
    public ResponseEntity<String> resumeJob(
            @RequestParam("name") String name,
            @RequestParam("group") String group) {

        quartzJobService.resumeJob(name, group);
        return ResponseEntity.ok("선택한 Job 스케줄이 정상적으로 활성화(RESUME)되었습니다.");
    }

    @PostMapping("/jobs/trigger")
    public ResponseEntity<String> triggerJob(
            @RequestParam("name") String name,
            @RequestParam("group") String group,
            @RequestBody(required = false) Map<String, Object> extraParams) {

        quartzJobService.triggerJob(name, group, extraParams);
        return ResponseEntity.ok("선택한 Job을 즉시 1회 수동 실행 요청하였습니다.");
    }
}
