package com.hunglp.consumer.saga.steps;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.saga.SagaState;
import com.hunglp.consumer.saga.SagaStep;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Payment Step: Process payment
 */
@Component
@Slf4j
public class PaymentStep implements SagaStep {

    @Override
    public boolean execute(OrderDto order) {
        log.info("Saga - Payment Step: Executing for orderId={}, amount={}", 
                order.getOrderId(), order.getTotalAmount());
        
        // Simulate payment processing
        // In real application, this would:
        // 1. Call payment gateway
        // 2. Process payment
        // 3. Update payment records
        
        boolean success = processPayment(order);
        
        if (success) {
            order.setStatus("PAID");
            log.info("Saga - Payment Step: Payment processed successfully for orderId={}", order.getOrderId());
            return true;
        } else {
            log.warn("Saga - Payment Step: Payment failed for orderId={}", order.getOrderId());
            return false;
        }
    }

    @Override
    public boolean compensate(OrderDto order) {
        log.info("Saga - Payment Step: Compensating (refunding payment) for orderId={}", order.getOrderId());
        
        // Refund payment
        // In real application, this would:
        // 1. Call payment gateway to refund
        // 2. Update payment records
        // 3. Update order status
        
        order.setStatus("REFUNDED");
        log.info("Saga - Payment Step: Refund completed for orderId={}", order.getOrderId());
        return true;
    }

    @Override
    public SagaState getSuccessState() {
        return SagaState.PAYMENT_PROCESSED;
    }

    @Override
    public String getStepName() {
        return "PAYMENT_STEP";
    }

    private boolean processPayment(OrderDto order) {
        // Simulate payment processing - 85% success rate
        boolean success = ThreadLocalRandom.current().nextDouble() > 0.15;
        log.debug("Saga - Payment processing result for order {}: {}", order.getOrderId(), success);
        
        // Simulate processing delay
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return success;
    }
}
