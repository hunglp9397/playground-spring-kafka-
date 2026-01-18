package com.hunglp.producer.api;

import com.hunglp.producer.dto.Message;
import com.hunglp.producer.dto.ReplyMessage;
import com.hunglp.producer.dto.SendMessageRequest;
import com.hunglp.producer.service.IdempotentMessageService;
import com.hunglp.producer.service.ReplyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/v1/high-performance")
@RequiredArgsConstructor
@Slf4j
public class HighPerformanceController {

    private final IdempotentMessageService messageService;
    private final ReplyService replyService;

    /**
     * Send single message với idempotency và exactly-once
     */
    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> sendMessage(@RequestBody SendMessageRequest request) {
        Message message = messageService.sendMessage(
                request.getTopic(),
                request.getKey(),
                request.getContent()
        );
        
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", message.getMessageId());
        response.put("key", message.getKey());
        response.put("partition", message.getPartition());
        response.put("status", "SENT");
        
        return ResponseEntity.ok(response);
    }

    /**
     * Send batch messages với high performance
     */
    @PostMapping("/send/batch")
    public ResponseEntity<Map<String, Object>> sendBatchMessages(
            @RequestParam String topic,
            @RequestParam(defaultValue = "1000") int count) {
        
        long startTime = System.currentTimeMillis();
        
        Map<String, String> messages = new HashMap<>();
        for (int i = 0; i < count; i++) {
            messages.put("key-" + i, "Message content " + i);
        }
        
        int sentCount = messageService.sendBatchMessages(topic, messages);
        
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (sentCount * 1000.0) / duration; // messages per second
        
        Map<String, Object> response = new HashMap<>();
        response.put("sentCount", sentCount);
        response.put("durationMs", duration);
        response.put("throughput", String.format("%.2f msg/s", throughput));
        
        return ResponseEntity.ok(response);
    }

    /**
     * Request-Reply pattern
     */
    @PostMapping("/request-reply")
    public ResponseEntity<Map<String, Object>> requestReply(@RequestBody SendMessageRequest request) {
        String replyTopic = "reply.topic";
        
        // Send request message
        Message requestMessage = messageService.sendRequestMessage(
                request.getTopic(),
                request.getKey(),
                request.getContent(),
                replyTopic
        );
        
        // Wait for reply
        long timeout = request.getTimeoutMs() != null ? request.getTimeoutMs() : 5000;
        CompletableFuture<ReplyMessage> replyFuture = replyService.waitForReply(
                requestMessage.getCorrelationId(),
                timeout
        );
        
        try {
            ReplyMessage reply = replyFuture.get(timeout, TimeUnit.MILLISECONDS);
            
            Map<String, Object> response = new HashMap<>();
            response.put("requestMessageId", requestMessage.getMessageId());
            response.put("correlationId", requestMessage.getCorrelationId());
            response.put("reply", reply);
            response.put("status", "SUCCESS");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("requestMessageId", requestMessage.getMessageId());
            response.put("correlationId", requestMessage.getCorrelationId());
            response.put("status", "TIMEOUT");
            response.put("error", e.getMessage());
            
            return ResponseEntity.status(408).body(response);
        }
    }

    /**
     * Performance test endpoint
     */
    @PostMapping("/performance-test")
    public ResponseEntity<Map<String, Object>> performanceTest(
            @RequestParam String topic,
            @RequestParam(defaultValue = "10000") int messageCount,
            @RequestParam(defaultValue = "100") int batchSize) {
        
        log.info("Starting performance test: topic={}, count={}, batchSize={}",
                topic, messageCount, batchSize);
        
        long startTime = System.currentTimeMillis();
        int sentCount = 0;
        
        // Send in batches
        for (int batch = 0; batch < messageCount / batchSize; batch++) {
            Map<String, String> batchMessages = new HashMap<>();
            for (int i = 0; i < batchSize; i++) {
                int messageIndex = batch * batchSize + i;
                batchMessages.put("key-" + messageIndex, "Message " + messageIndex);
            }
            
            sentCount += messageService.sendBatchMessages(topic, batchMessages);
        }
        
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (sentCount * 1000.0) / duration;
        
        Map<String, Object> result = new HashMap<>();
        result.put("topic", topic);
        result.put("messageCount", messageCount);
        result.put("sentCount", sentCount);
        result.put("durationMs", duration);
        result.put("throughput", String.format("%.2f msg/s", throughput));
        result.put("avgLatencyMs", duration / (double) sentCount);
        
        log.info("Performance test completed: throughput={} msg/s", throughput);
        
        return ResponseEntity.ok(result);
    }
}
