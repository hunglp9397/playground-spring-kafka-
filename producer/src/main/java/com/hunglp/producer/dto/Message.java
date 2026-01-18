package com.hunglp.producer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Message DTO với unique message ID để đảm bảo idempotency
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private String messageId; // Unique ID để đảm bảo idempotency
    private String key; // Key để đảm bảo ordering
    private String content;
    private String correlationId; // Cho request-reply pattern
    private String replyTopic; // Topic để gửi reply
    private LocalDateTime timestamp;
    private Integer partition; // Partition được chọn (for logging)

    public Message(String key, String content) {
        this.messageId = UUID.randomUUID().toString();
        this.key = key;
        this.content = content;
        this.timestamp = LocalDateTime.now();
    }

    public Message(String key, String content, String correlationId, String replyTopic) {
        this(key, content);
        this.correlationId = correlationId;
        this.replyTopic = replyTopic;
    }
}
