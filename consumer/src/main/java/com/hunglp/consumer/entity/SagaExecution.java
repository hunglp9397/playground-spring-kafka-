package com.hunglp.consumer.entity;

import com.hunglp.consumer.saga.SagaState;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Saga Execution entity to track saga state in database
 */
@Entity
@Table(name = "saga_executions")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SagaExecution {

    @Id
    @Column(name = "saga_id", length = 36)
    private String sagaId;

    @Column(name = "order_id", nullable = false, length = 36)
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_state", nullable = false, length = 50)
    private SagaState currentState;

    @Column(name = "completed_steps", length = 500)
    private String completedSteps; // Comma-separated list of completed step names

    @Column(name = "failed_step", length = 100)
    private String failedStep;

    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public void addCompletedStep(String stepName) {
        if (completedSteps == null || completedSteps.isEmpty()) {
            completedSteps = stepName;
        } else {
            completedSteps += "," + stepName;
        }
    }
}
