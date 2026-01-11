package com.hunglp.consumer.api;

import com.hunglp.consumer.dto.AnalyticsMetrics;
import com.hunglp.consumer.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    @GetMapping("/metrics")
    public ResponseEntity<AnalyticsMetrics> getMetrics(
            @RequestParam(defaultValue = "60") int minutes) {
        Duration windowDuration = Duration.ofMinutes(minutes);
        AnalyticsMetrics metrics = analyticsService.getMetricsForWindow(windowDuration);
        return ResponseEntity.ok(metrics);
    }

    @GetMapping("/metrics/realtime")
    public ResponseEntity<AnalyticsMetrics> getRealtimeMetrics() {
        // Last 1 minute
        Duration windowDuration = Duration.ofMinutes(1);
        AnalyticsMetrics metrics = analyticsService.getMetricsForWindow(windowDuration);
        return ResponseEntity.ok(metrics);
    }

    @GetMapping("/metrics/hourly")
    public ResponseEntity<AnalyticsMetrics> getHourlyMetrics() {
        Duration windowDuration = Duration.ofHours(1);
        AnalyticsMetrics metrics = analyticsService.getMetricsForWindow(windowDuration);
        return ResponseEntity.ok(metrics);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "analytics-consumer"));
    }
}
