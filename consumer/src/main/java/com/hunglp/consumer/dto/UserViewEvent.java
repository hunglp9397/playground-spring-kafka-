package com.hunglp.consumer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class UserViewEvent extends UserEvent {
    private String pageUrl;
    private String pageTitle;
    private Long duration;
    private String referrer;
}
