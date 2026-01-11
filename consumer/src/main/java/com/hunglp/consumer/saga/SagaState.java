package com.hunglp.consumer.saga;

/**
 * Saga State enum represents the current state of a distributed transaction
 * in the order processing saga
 */
public enum SagaState {
    /**
     * Saga started, order created
     */
    ORDER_CREATED,
    
    /**
     * Inventory check completed successfully
     */
    INVENTORY_RESERVED,
    
    /**
     * Payment processed successfully
     */
    PAYMENT_PROCESSED,
    
    /**
     * Notification sent successfully
     */
    NOTIFICATION_SENT,
    
    /**
     * Saga completed successfully
     */
    COMPLETED,
    
    /**
     * Saga failed and compensation started
     */
    COMPENSATING,
    
    /**
     * Saga failed and all compensations completed
     */
    COMPENSATED,
    
    /**
     * Saga failed permanently
     */
    FAILED
}
