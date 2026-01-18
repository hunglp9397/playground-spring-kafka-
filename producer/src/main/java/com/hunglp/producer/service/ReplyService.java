package com.hunglp.producer.service;

import com.hunglp.producer.dto.Message;
import com.hunglp.producer.dto.ReplyMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reply Service cho Request-Reply Pattern
 * 
 * Lưu trữ pending requests và match với replies
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ReplyService {

    private final org.springframework.kafka.core.KafkaTemplate<String, ReplyMessage> replyKafkaTemplate;
    
    // Store pending requests: correlationId -> CompletableFuture
    private final Map<String, CompletableFuture<ReplyMessage>> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Wait for reply message
     */
    public CompletableFuture<ReplyMessage> waitForReply(String correlationId, long timeoutMs) {
        CompletableFuture<ReplyMessage> future = new CompletableFuture<>();
        pendingRequests.put(correlationId, future);
        
        // Timeout handling
        CompletableFuture.delayedExecutor(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                .execute(() -> {
                    if (pendingRequests.remove(correlationId) != null) {
                        future.completeExceptionally(new java.util.concurrent.TimeoutException(
                                "Reply timeout after " + timeoutMs + "ms"));
                    }
                });
        
        return future;
    }

    /**
     * Consumer để nhận reply messages
     */
    @KafkaListener(
            topics = "${kafka.reply.topic:reply.topic}",
            groupId = "reply-service-group"
    )
    public void receiveReply(@Payload ReplyMessage reply,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                           @Header(KafkaHeaders.OFFSET) long offset,
                           Acknowledgment acknowledgment) {
        try {
            log.info("Received reply: correlationId={}, status={}, partition={}, offset={}",
                    reply.getCorrelationId(), reply.getStatus(), partition, offset);
            
            // Match với pending request
            CompletableFuture<ReplyMessage> future = pendingRequests.remove(reply.getCorrelationId());
            if (future != null) {
                future.complete(reply);
                log.info("Reply matched with pending request: correlationId={}", reply.getCorrelationId());
            } else {
                log.warn("No pending request found for correlationId: {}", reply.getCorrelationId());
            }
            
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error processing reply: correlationId={}", reply.getCorrelationId(), e);
        }
    }

    /**
     * Send reply message
     */
    public void sendReply(String replyTopic, String correlationId, String replyContent, String status) {
        ReplyMessage reply = new ReplyMessage();
        reply.setCorrelationId(correlationId);
        reply.setReplyContent(replyContent);
        reply.setStatus(status);
        reply.setTimestamp(LocalDateTime.now());
        
        replyKafkaTemplate.send(replyTopic, correlationId, reply)
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        log.info("Reply sent: correlationId={}, topic={}, partition={}, offset={}",
                                correlationId,
                                replyTopic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to send reply: correlationId={}", correlationId, exception);
                    }
                });
    }
}
