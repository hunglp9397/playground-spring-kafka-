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
public class UserClickEvent extends UserEvent {
    private String pageUrl;
    private String elementId;
    private String elementType; // button, link, image, etc.
    private String referrer;

    public UserClickEvent(String userId, String sessionId, String pageUrl, 
                         String elementId, String elementType, String referrer) {
        super(userId, sessionId, LocalDateTime.now(), "CLICK");
        this.pageUrl = pageUrl;
        this.elementId = elementId;
        this.elementType = elementType;
        this.referrer = referrer;
    }
}
