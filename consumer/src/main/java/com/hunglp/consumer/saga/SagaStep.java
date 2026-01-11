package com.hunglp.consumer.saga;

import com.hunglp.consumer.dto.OrderDto;

/**
 * Saga Step interface for saga workflow steps
 */
public interface SagaStep {
    
    /**
     * Execute the saga step
     * @param order Order to process
     * @return true if successful, false otherwise
     */
    boolean execute(OrderDto order);
    
    /**
     * Compensate (rollback) the saga step
     * @param order Order to compensate
     * @return true if compensation successful, false otherwise
     */
    boolean compensate(OrderDto order);
    
    /**
     * Get the state this step transitions to on success
     */
    SagaState getSuccessState();
    
    /**
     * Get the step name
     */
    String getStepName();
}
