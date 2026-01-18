package com.hunglp.producer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReplyMessage {
    private String correlationId;
    private String replyContent;
    private LocalDateTime timestamp;
    private String status; // SUCCESS, ERROR
}
