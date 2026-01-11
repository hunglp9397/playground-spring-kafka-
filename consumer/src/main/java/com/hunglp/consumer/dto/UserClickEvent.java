package com.hunglp.consumer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class UserClickEvent extends UserEvent {
    private String pageUrl;
    private String elementId;
    private String elementType;
    private String referrer;
}
