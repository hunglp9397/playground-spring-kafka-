package com.hunglp.consumer.api;

import com.hunglp.consumer.service.IdempotentConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/consumer")
@RequiredArgsConstructor
public class ConsumerController {

    private final IdempotentConsumerService consumerService;

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStatistics() {
        return ResponseEntity.ok(consumerService.getStatistics());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "high-performance-consumer"));
    }
}
