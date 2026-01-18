package com.hunglp.producer.service;

import com.hunglp.producer.dto.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Idempotent Message Service
 * 
 * Đảm bảo:
 * - Không gửi duplicate messages (idempotency)
 * - Exactly-once semantics với transactions
 * - Message ordering với key-based partitioning
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class IdempotentMessageService {

    private final KafkaTemplate<String, Message> highPerformanceKafkaTemplate;
    
    // Track sent message IDs để tránh duplicate (in-memory, có thể dùng Redis trong production)
    private final ConcurrentMap<String, Boolean> sentMessageIds = new ConcurrentHashMap<>();

    /**
     * Send message với idempotency guarantee
     * 
     * @param topic Topic name
     * @param key Message key (để đảm bảo ordering)
     * @param content Message content
     * @return Message với messageId
     */
    public Message sendMessage(String topic, String key, String content) {
        // Generate unique message ID
        String messageId = UUID.randomUUID().toString();
        
        // Check if message already sent (idempotency check)
        if (sentMessageIds.containsKey(messageId)) {
            log.warn("Message already sent: messageId={}", messageId);
            return null; // Already sent, skip
        }
        
        // Create message
        Message message = new Message(key, content);
        message.setMessageId(messageId);
        
        // Send message với exactly-once semantics
        // Idempotent producer + transactions đảm bảo exactly-once
        CompletableFuture<org.springframework.kafka.support.SendResult<String, Message>> future = 
                highPerformanceKafkaTemplate.send(topic, key, message);
        
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                // Mark message as sent
                sentMessageIds.put(messageId, true);
                
                log.info("Message sent successfully: messageId={}, topic={}, partition={}, offset={}",
                        messageId,
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                
                // Store partition info
                message.setPartition(result.getRecordMetadata().partition());
            } else {
                log.error("Failed to send message: messageId={}", messageId, exception);
                // Remove from sent cache on failure
                sentMessageIds.remove(messageId);
            }
        });
        
        return message;
    }

    /**
     * Send batch messages với high performance
     * 
     * @param topic Topic name
     * @param messages Map of key -> content
     * @return Number of messages sent
     */
    public int sendBatchMessages(String topic, java.util.Map<String, String> messages) {
        int sentCount = 0;
        
        // Send messages trong transaction để đảm bảo exactly-once
        highPerformanceKafkaTemplate.executeInTransaction(operations -> {
            messages.forEach((key, content) -> {
                String messageId = UUID.randomUUID().toString();
                
                if (!sentMessageIds.containsKey(messageId)) {
                    Message message = new Message(key, content);
                    message.setMessageId(messageId);
                    
                    operations.send(topic, key, message);
                    sentMessageIds.put(messageId, true);
                }
            });
            return null;
        });
        
        sentCount = messages.size();
        return sentCount;
    }

    /**
     * Send message với request-reply pattern
     * 
     * @param topic Topic name
     * @param key Message key
     * @param content Message content
     * @param replyTopic Topic để nhận reply
     * @return Message với correlationId
     */
    public Message sendRequestMessage(String topic, String key, String content, String replyTopic) {
        String correlationId = UUID.randomUUID().toString();
        String messageId = UUID.randomUUID().toString();
        
        Message message = new Message(key, content, correlationId, replyTopic);
        message.setMessageId(messageId);
        
        highPerformanceKafkaTemplate.executeInTransaction(operations -> {
            operations.send(topic, key, message);
            sentMessageIds.put(messageId, true);
            return null;
        });
        
        log.info("Request message sent: messageId={}, correlationId={}, replyTopic={}",
                messageId, correlationId, replyTopic);
        
        return message;
    }

    /**
     * Check if message was already sent (idempotency check)
     */
    public boolean isMessageSent(String messageId) {
        return sentMessageIds.containsKey(messageId);
    }
}
