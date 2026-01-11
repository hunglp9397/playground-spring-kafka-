package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.OrderDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

/**
 * Service demonstrating @Retryable annotation approach
 * This is an alternative to using RetryTemplate programmatically
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RetryableOrderProcessor {

    /**
     * Process order with automatic retry using @Retryable annotation
     * 
     * Retry configuration:
     * - maxAttempts: 3 (initial + 2 retries)
     * - backoff: exponential with initial delay 1s, multiplier 2.0, max delay 10s
     * 
     * @param order Order to process
     * @throws Exception if processing fails after all retries
     */
    @Retryable(
            value = {Exception.class},
            maxAttempts = 3,
            backoff = @Backoff(
                    delay = 1000,        // Initial delay: 1 second
                    multiplier = 2.0,   // Double the delay each retry
                    maxDelay = 10000     // Max delay: 10 seconds
            )
    )
    public void processOrderWithRetry(OrderDto order) throws Exception {
        log.info("Processing order with retry: orderId={}", order.getOrderId());
        
        // Simulate processing that might fail
        if (Math.random() < 0.3) { // 30% chance of failure for demo
            throw new RuntimeException("Simulated processing failure");
        }
        
        log.info("Order processed successfully: orderId={}", order.getOrderId());
    }

    /**
     * Recovery method called after all retries are exhausted
     * This method will be called if processOrderWithRetry fails after all retries
     */
    @Recover
    public void recover(Exception ex, OrderDto order) {
        log.error("All retry attempts exhausted for order: orderId={}, error={}",
                order.getOrderId(), ex.getMessage());
        // In real application, send to DLQ or handle recovery logic here
    }
}
