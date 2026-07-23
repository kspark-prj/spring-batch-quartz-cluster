package com.example.demo.quartz.controller;

import com.example.demo.quartz.dto.JobRequest;
import com.example.demo.quartz.dto.SchedulerStatusResponse;
import com.example.demo.quartz.job.SampleBatchTriggerJob;
import com.example.demo.quartz.job.SampleSystemMonitoringJob;
import com.example.demo.quartz.service.QuartzJobService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Quartz 스케줄러 동적 관리를 위한 REST Controller입니다.
 */
@RestController
@RequestMapping("/api/quartz")
public class QuartzJobController {

    private final QuartzJobService quartzJobService;

    public QuartzJobController(QuartzJobService quartzJobService) {
        this.quartzJobService = quartzJobService;
    }

    @GetMapping("/status")
    public ResponseEntity<SchedulerStatusResponse> getStatus() {
        return ResponseEntity.ok(quartzJobService.getSchedulerStatus());
    }

    @PostMapping("/jobs")
    public ResponseEntity<String> createJob(
            @RequestBody JobRequest request,
            @RequestParam(value = "type", defaultValue = "BATCH") String type) {
        
        Class<? extends org.quartz.Job> targetJobClass = 
                "MONITOR".equalsIgnoreCase(type) ? SampleSystemMonitoringJob.class : SampleBatchTriggerJob.class;

        boolean created = quartzJobService.addJob(request, targetJobClass);
        if (created) {
            return ResponseEntity.ok("Quartz 스케줄 Job이 성공적으로 등록되었습니다.");
        } else {
            return ResponseEntity.badRequest().body("이미 등록된 동일한 이름과 그룹의 Job이 존재합니다.");
        }
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
