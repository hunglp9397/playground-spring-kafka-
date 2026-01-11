package com.hunglp.consumer.saga;

import com.hunglp.consumer.dto.OrderDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Saga Event represents an event in the saga workflow
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SagaEvent {
    private String sagaId;
    private String orderId;
    private SagaState currentState;
    private SagaState previousState;
    private String eventType; // STEP_COMPLETED, STEP_FAILED, COMPENSATION_COMPLETED
    private OrderDto order;
    private String errorMessage;
    private LocalDateTime timestamp;
}
