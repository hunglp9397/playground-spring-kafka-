package com.hunglp.producer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class UserViewEvent extends UserEvent {
    private String pageUrl;
    private String pageTitle;
    private Long duration; // milliseconds
    private String referrer;

    public UserViewEvent(String userId, String sessionId, String pageUrl, 
                       String pageTitle, Long duration, String referrer) {
        super(userId, sessionId, LocalDateTime.now(), "VIEW");
        this.pageUrl = pageUrl;
        this.pageTitle = pageTitle;
        this.duration = duration;
        this.referrer = referrer;
    }
}
