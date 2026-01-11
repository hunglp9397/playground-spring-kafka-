package com.hunglp.consumer.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = UserClickEvent.class, name = "CLICK"),
    @JsonSubTypes.Type(value = UserViewEvent.class, name = "VIEW"),
    @JsonSubTypes.Type(value = UserPurchaseEvent.class, name = "PURCHASE")
})
public abstract class UserEvent {
    private String userId;
    private String sessionId;
    private LocalDateTime timestamp;
    private String eventType;
}
