package com.hunglp.producer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SendMessageRequest {
    private String key; // Key để đảm bảo ordering
    private String content;
    private String topic;
    private Boolean waitForReply; // Cho request-reply pattern
    private Long timeoutMs; // Timeout cho reply (milliseconds)
}
