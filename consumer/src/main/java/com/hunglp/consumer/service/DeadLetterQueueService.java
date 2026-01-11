package com.hunglp.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hunglp.consumer.dto.OrderDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dead Letter Queue Service - handles messages that failed processing after retries
 * In production, this would typically send to a DLQ topic for manual investigation
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DeadLetterQueueService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    // In-memory storage for failed messages (for demo purposes)
    // In production, use a database or send to DLQ topic
    private final Map<String, OrderDto> failedMessages = new ConcurrentHashMap<>();

    /**
     * This method would be called when a message fails processing after max retries
     * For demo purposes, we'll manually send failed messages to DLQ
     */
    public void sendToDLQ(String orderId, OrderDto order, String reason) {
        try {
            String dlqMessage = String.format(
                    "Failed to process order. OrderId: %s, Reason: %s, Order: %s",
                    orderId, reason, objectMapper.writeValueAsString(order)
            );

            kafkaTemplate.send("dlq.order.failed", orderId, dlqMessage)
                    .whenComplete((result, exception) -> {
                        if (exception == null) {
                            log.warn("DLQ - Message sent to dead letter queue: orderId={}, reason={}",
                                    orderId, reason);
                            failedMessages.put(orderId, order);
                        } else {
                            log.error("DLQ - Failed to send message to DLQ: orderId={}", orderId, exception);
                        }
                    });
        } catch (Exception e) {
            log.error("DLQ - Error sending message to DLQ: orderId={}", orderId, e);
        }
    }

    @KafkaListener(
            topics = "dlq.order.failed",
            groupId = "dlq-processor-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processDLQMessage(@Payload String message,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                 @Header(KafkaHeaders.OFFSET) long offset,
                                 @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                 Acknowledgment acknowledgment) {
        try {
            log.error("DLQ - Processing failed message: key={}, message={}, partition={}, offset={}",
                    key, message, partition, offset);

            // In production, this would:
            // 1. Store in database for investigation
            // 2. Send alert to operations team
            // 3. Provide API to retry or manually fix messages

            log.error("DLQ - Failed message details: {}", message);

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("DLQ - Error processing DLQ message: key={}", key, e);
        }
    }

    public Map<String, OrderDto> getFailedMessages() {
        return failedMessages;
    }
}
