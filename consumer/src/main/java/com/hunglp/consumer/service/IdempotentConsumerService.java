package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Idempotent Consumer Service
 * 
 * Đảm bảo:
 * - Không xử lý duplicate messages (idempotency)
 * - Chỉ commit sau khi xử lý thành công
 * - Track processed messages để tránh duplicate
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class IdempotentConsumerService {

    private final org.springframework.kafka.core.KafkaTemplate<String, com.hunglp.consumer.dto.ReplyMessage> replyKafkaTemplate;
    
    // Track processed message IDs (in-memory, có thể dùng Redis trong production)
    private final ConcurrentMap<String, Boolean> processedMessageIds = new ConcurrentHashMap<>();

    /**
     * High-performance consumer với batch processing
     */
    @KafkaListener(
            topics = "${kafka.performance.topic:performance.topic}",
            groupId = "high-performance-consumer-group",
            containerFactory = "highPerformanceKafkaListenerContainerFactory"
    )
    public void processMessagesBatch(
            @Payload java.util.List<Message> messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION) java.util.List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) java.util.List<Long> offsets,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Processing batch of {} messages", messages.size());
            
            int processedCount = 0;
            int duplicateCount = 0;
            
            for (int i = 0; i < messages.size(); i++) {
                Message message = messages.get(i);
                int partition = partitions.get(i);
                long offset = offsets.get(i);
                
                // Idempotency check: Đã xử lý message này chưa?
                if (processedMessageIds.containsKey(message.getMessageId())) {
                    log.warn("Duplicate message detected and skipped: messageId={}, partition={}, offset={}",
                            message.getMessageId(), partition, offset);
                    duplicateCount++;
                    continue; // Skip duplicate
                }
                
                // Process message
                processMessage(message, partition, offset);
                
                // Mark as processed
                processedMessageIds.put(message.getMessageId(), true);
                processedCount++;
            }
            
            log.info("Batch processed: total={}, processed={}, duplicates={}",
                    messages.size(), processedCount, duplicateCount);
            
            // Chỉ commit sau khi tất cả messages được xử lý thành công
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("Error processing batch", e);
            // Không acknowledge → messages sẽ được retry
            throw e;
        }
    }

    /**
     * Single message consumer với ordering guarantee
     */
    @KafkaListener(
            topics = "${kafka.ordered.topic:ordered.topic}",
            groupId = "ordered-consumer-group",
            containerFactory = "highPerformanceKafkaListenerContainerFactory"
    )
    public void processOrderedMessage(
            @Payload Message message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Processing ordered message: messageId={}, key={}, partition={}, offset={}",
                    message.getMessageId(), key, partition, offset);
            
            // Idempotency check
            if (processedMessageIds.containsKey(message.getMessageId())) {
                log.warn("Duplicate message skipped: messageId={}", message.getMessageId());
                acknowledgment.acknowledge(); // Acknowledge duplicate để không retry
                return;
            }
            
            // Process message
            processMessage(message, partition, offset);
            
            // Mark as processed
            processedMessageIds.put(message.getMessageId(), true);
            
            // Commit sau khi xử lý thành công
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("Error processing ordered message: messageId={}", message.getMessageId(), e);
            // Không acknowledge → sẽ retry
            throw e;
        }
    }

    /**
     * Request-Reply pattern: Process request và send reply
     */
    @KafkaListener(
            topics = "${kafka.request.topic:request.topic}",
            groupId = "request-reply-consumer-group",
            containerFactory = "highPerformanceKafkaListenerContainerFactory"
    )
    public void processRequestMessage(
            @Payload Message request,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Processing request: messageId={}, correlationId={}, partition={}, offset={}",
                    request.getMessageId(), request.getCorrelationId(), partition, offset);
            
            // Idempotency check
            if (processedMessageIds.containsKey(request.getMessageId())) {
                log.warn("Duplicate request skipped: messageId={}", request.getMessageId());
                acknowledgment.acknowledge();
                return;
            }
            
            // Process request
            String replyContent = processRequest(request);
            
            // Send reply nếu có replyTopic
            if (request.getReplyTopic() != null && request.getCorrelationId() != null) {
                com.hunglp.consumer.dto.ReplyMessage reply = new com.hunglp.consumer.dto.ReplyMessage();
                reply.setCorrelationId(request.getCorrelationId());
                reply.setReplyContent(replyContent);
                reply.setStatus("SUCCESS");
                reply.setTimestamp(java.time.LocalDateTime.now());
                
                replyKafkaTemplate.send(request.getReplyTopic(), request.getCorrelationId(), reply)
                        .whenComplete((result, exception) -> {
                            if (exception == null) {
                                log.info("Reply sent: correlationId={}, topic={}, partition={}, offset={}",
                                        request.getCorrelationId(),
                                        request.getReplyTopic(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            } else {
                                log.error("Failed to send reply: correlationId={}",
                                        request.getCorrelationId(), exception);
                            }
                        });
            }
            
            // Mark as processed
            processedMessageIds.put(request.getMessageId(), true);
            
            // Commit
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("Error processing request: messageId={}", request.getMessageId(), e);
            throw e;
        }
    }

    /**
     * Process message (business logic)
     */
    private void processMessage(Message message, int partition, long offset) {
        // Simulate processing
        log.debug("Processing message: messageId={}, key={}, content={}, partition={}, offset={}",
                message.getMessageId(), message.getKey(), message.getContent(), partition, offset);
        
        // In real application, implement your business logic here
        // Ví dụ: Save to database, call external service, etc.
        
        // Simulate processing time
        try {
            Thread.sleep(10); // 10ms processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Process request và generate reply
     */
    private String processRequest(Message request) {
        log.debug("Processing request: messageId={}, content={}",
                request.getMessageId(), request.getContent());
        
        // Simulate request processing
        try {
            Thread.sleep(50); // 50ms processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Generate reply
        return "Processed: " + request.getContent();
    }

    /**
     * Check if message was already processed
     */
    public boolean isMessageProcessed(String messageId) {
        return processedMessageIds.containsKey(messageId);
    }

    /**
     * Get statistics
     */
    public java.util.Map<String, Object> getStatistics() {
        java.util.Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("processedMessages", processedMessageIds.size());
        return stats;
    }
}
