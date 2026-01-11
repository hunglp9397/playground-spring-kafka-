package com.hunglp.consumer.saga.steps;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.saga.SagaState;
import com.hunglp.consumer.saga.SagaStep;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Notification Step: Send notification
 * This step is typically non-compensatable (idempotent)
 */
@Component
@Slf4j
public class NotificationStep implements SagaStep {

    @Override
    public boolean execute(OrderDto order) {
        log.info("Saga - Notification Step: Executing for orderId={}", order.getOrderId());
        
        // Send notification
        // In real application, this would:
        // 1. Send email/SMS notification
        // 2. Update notification records
        
        sendNotification(order);
        log.info("Saga - Notification Step: Notification sent successfully for orderId={}", order.getOrderId());
        return true;
    }

    @Override
    public boolean compensate(OrderDto order) {
        log.info("Saga - Notification Step: Compensation (notification cannot be unsent, but can send cancellation)");
        
        // Send cancellation notification
        // Notifications are typically non-compensatable
        // But we can send a cancellation notification
        
        sendCancellationNotification(order);
        log.info("Saga - Notification Step: Cancellation notification sent for orderId={}", order.getOrderId());
        return true;
    }

    @Override
    public SagaState getSuccessState() {
        return SagaState.NOTIFICATION_SENT;
    }

    @Override
    public String getStepName() {
        return "NOTIFICATION_STEP";
    }

    private void sendNotification(OrderDto order) {
        String emailContent = String.format(
                "Dear Customer,\n\n" +
                "Your order has been confirmed!\n\n" +
                "Order ID: %s\n" +
                "Total Amount: %s\n\n" +
                "Thank you for your purchase!",
                order.getOrderId(),
                order.getTotalAmount()
        );
        log.info("Saga - Notification sent to user {}:\n{}", order.getUserId(), emailContent);
    }

    private void sendCancellationNotification(OrderDto order) {
        String emailContent = String.format(
                "Dear Customer,\n\n" +
                "Your order has been cancelled.\n\n" +
                "Order ID: %s\n" +
                "Total Amount: %s\n\n" +
                "Refund will be processed shortly.",
                order.getOrderId(),
                order.getTotalAmount()
        );
        log.info("Saga - Cancellation notification sent to user {}:\n{}", order.getUserId(), emailContent);
    }
}
