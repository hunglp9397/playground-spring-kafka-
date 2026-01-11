package com.hunglp.producer.service;

import com.hunglp.producer.dto.UserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class AnalyticsEventService {

    private final KafkaTemplate<String, Object> eventKafkaTemplate;

    public void sendEvent(String topic, String key, UserEvent event) {
        eventKafkaTemplate.send(topic, key, event)
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        log.debug("Event sent successfully: topic={}, key={}, eventType={}, partition={}, offset={}",
                                topic, key, event.getEventType(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to send event: topic={}, key={}, eventType={}",
                                topic, key, event.getEventType(), exception);
                    }
                });
    }

    public void sendClickEvent(UserEvent event) {
        sendEvent("analytics.user.click", event.getUserId(), event);
    }

    public void sendViewEvent(UserEvent event) {
        sendEvent("analytics.user.view", event.getUserId(), event);
    }

    public void sendPurchaseEvent(UserEvent event) {
        sendEvent("analytics.user.purchase", event.getUserId(), event);
    }
}
