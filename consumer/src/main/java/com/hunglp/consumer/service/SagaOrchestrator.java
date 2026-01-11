package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.entity.SagaExecution;
import com.hunglp.consumer.repository.SagaExecutionRepository;
import com.hunglp.consumer.saga.SagaEvent;
import com.hunglp.consumer.saga.SagaState;
import com.hunglp.consumer.saga.SagaStep;
import com.hunglp.consumer.saga.steps.InventoryStep;
import com.hunglp.consumer.saga.steps.NotificationStep;
import com.hunglp.consumer.saga.steps.PaymentStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Saga Orchestrator: Orchestrates the distributed transaction
 * Implements Orchestration-based Saga Pattern
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final InventoryStep inventoryStep;
    private final PaymentStep paymentStep;
    private final NotificationStep notificationStep;
    private final SagaExecutionRepository sagaExecutionRepository;
    private final KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate;
    private final KafkaTemplate<String, OrderDto> orderKafkaTemplate;

    /**
     * Start saga orchestration when order is created
     */
    @KafkaListener(
            topics = "order.created",
            groupId = "saga-orchestrator-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void startSaga(@Payload OrderDto order,
                         @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                         @Header(KafkaHeaders.OFFSET) long offset,
                         @Header(KafkaHeaders.RECEIVED_KEY) String key,
                         Acknowledgment acknowledgment) {
        try {
            log.info("Saga Orchestrator - Starting saga for orderId={}, partition={}, offset={}",
                    order.getOrderId(), partition, offset);

            String sagaId = UUID.randomUUID().toString();
            
            // Create saga execution record
            SagaExecution sagaExecution = new SagaExecution();
            sagaExecution.setSagaId(sagaId);
            sagaExecution.setOrderId(order.getOrderId());
            sagaExecution.setCurrentState(SagaState.ORDER_CREATED);
            sagaExecutionRepository.save(sagaExecution);

            // Execute saga steps
            boolean success = executeSaga(sagaId, order);

            if (success) {
                log.info("Saga Orchestrator - Saga completed successfully: sagaId={}, orderId={}",
                        sagaId, order.getOrderId());
                
                // Update saga state
                sagaExecution.setCurrentState(SagaState.COMPLETED);
                sagaExecutionRepository.save(sagaExecution);
                
                // Send completion event
                sendSagaEvent(sagaId, order, SagaState.COMPLETED, SagaState.NOTIFICATION_SENT, "SAGA_COMPLETED");
            } else {
                log.error("Saga Orchestrator - Saga failed: sagaId={}, orderId={}",
                        sagaId, order.getOrderId());
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Saga Orchestrator - Error processing saga: orderId={}", order.getOrderId(), e);
            throw e;
        }
    }

    /**
     * Execute saga steps in sequence
     */
    @Transactional
    private boolean executeSaga(String sagaId, OrderDto order) {
        List<SagaStep> steps = Arrays.asList(inventoryStep, paymentStep, notificationStep);
        List<SagaStep> executedSteps = new ArrayList<>();
        
        SagaExecution sagaExecution = sagaExecutionRepository.findBySagaId(sagaId)
                .orElseThrow(() -> new RuntimeException("Saga execution not found: " + sagaId));

        try {
            for (SagaStep step : steps) {
                log.info("Saga Orchestrator - Executing step: {} for orderId={}", 
                        step.getStepName(), order.getOrderId());

                boolean stepSuccess = step.execute(order);

                if (!stepSuccess) {
                    log.error("Saga Orchestrator - Step failed: {} for orderId={}", 
                            step.getStepName(), order.getOrderId());
                    
                    sagaExecution.setCurrentState(SagaState.COMPENSATING);
                    sagaExecution.setFailedStep(step.getStepName());
                    sagaExecution.setErrorMessage("Step failed: " + step.getStepName());
                    sagaExecutionRepository.save(sagaExecution);

                    // Compensate executed steps in reverse order
                    compensateSteps(executedSteps, order);
                    
                    sagaExecution.setCurrentState(SagaState.COMPENSATED);
                    sagaExecutionRepository.save(sagaExecution);
                    
                    // Send failure event
                    sendSagaEvent(sagaId, order, SagaState.COMPENSATED, 
                            sagaExecution.getCurrentState(), "SAGA_FAILED");
                    
                    // Send order cancelled event
                    order.setStatus("CANCELLED");
                    orderKafkaTemplate.send("order.cancelled", order.getOrderId(), order);
                    
                    return false;
                }

                // Step succeeded
                executedSteps.add(step);
                sagaExecution.setCurrentState(step.getSuccessState());
                sagaExecution.addCompletedStep(step.getStepName());
                sagaExecutionRepository.save(sagaExecution);

                // Send step completed event
                sendSagaEvent(sagaId, order, step.getSuccessState(), 
                        sagaExecution.getCurrentState(), "STEP_COMPLETED");

                log.info("Saga Orchestrator - Step completed: {} for orderId={}", 
                        step.getStepName(), order.getOrderId());
            }

            // All steps completed successfully
            return true;

        } catch (Exception e) {
            log.error("Saga Orchestrator - Exception during saga execution: sagaId={}, orderId={}",
                    sagaId, order.getOrderId(), e);

            sagaExecution.setCurrentState(SagaState.COMPENSATING);
            sagaExecution.setErrorMessage(e.getMessage());
            sagaExecutionRepository.save(sagaExecution);

            // Compensate executed steps
            compensateSteps(executedSteps, order);
            
            sagaExecution.setCurrentState(SagaState.COMPENSATED);
            sagaExecutionRepository.save(sagaExecution);
            
            return false;
        }
    }

    /**
     * Compensate executed steps in reverse order
     */
    private void compensateSteps(List<SagaStep> executedSteps, OrderDto order) {
        log.info("Saga Orchestrator - Starting compensation for {} steps", executedSteps.size());

        // Compensate in reverse order (LIFO)
        Collections.reverse(executedSteps);

        for (SagaStep step : executedSteps) {
            try {
                log.info("Saga Orchestrator - Compensating step: {}", step.getStepName());
                boolean compensationSuccess = step.compensate(order);
                
                if (!compensationSuccess) {
                    log.error("Saga Orchestrator - Compensation failed for step: {}", step.getStepName());
                    // In production, might want to retry or alert
                }
            } catch (Exception e) {
                log.error("Saga Orchestrator - Exception during compensation for step: {}", 
                        step.getStepName(), e);
            }
        }

        log.info("Saga Orchestrator - Compensation completed");
    }

    /**
     * Send saga event to Kafka
     */
    private void sendSagaEvent(String sagaId, OrderDto order, SagaState currentState, 
                               SagaState previousState, String eventType) {
        SagaEvent event = new SagaEvent();
        event.setSagaId(sagaId);
        event.setOrderId(order.getOrderId());
        event.setCurrentState(currentState);
        event.setPreviousState(previousState);
        event.setEventType(eventType);
        event.setOrder(order);
        event.setTimestamp(LocalDateTime.now());

        sagaEventKafkaTemplate.send("saga.events", sagaId, event)
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        log.debug("Saga Orchestrator - Saga event sent: sagaId={}, eventType={}",
                                sagaId, eventType);
                    } else {
                        log.error("Saga Orchestrator - Failed to send saga event: sagaId={}",
                                sagaId, exception);
                    }
                });
    }
}
