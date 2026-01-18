package com.hunglp.consumer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private String messageId;
    private String key;
    private String content;
    private String correlationId;
    private String replyTopic;
    private LocalDateTime timestamp;
    private Integer partition;
}
