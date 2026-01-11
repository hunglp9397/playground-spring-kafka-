package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.OrderDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    private final RetryTemplate retryTemplate;

    @KafkaListener(
            topics = "order.created",
            groupId = "notification-service-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void sendOrderConfirmationEmail(@Payload OrderDto order,
                                         @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                         @Header(KafkaHeaders.OFFSET) long offset,
                                         @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                         Acknowledgment acknowledgment) {
        try {
            log.info("Notification Service - Sending order confirmation email: orderId={}, userId={}, partition={}, offset={}",
                    order.getOrderId(), order.getUserId(), partition, offset);

            // Use RetryTemplate for retry with exponential backoff
            retryTemplate.execute(context -> {
                // Simulate sending email that might fail with transient errors
                sendEmailWithRetry(order);
                return null;
            });

            log.info("Notification Service - Order confirmation email sent successfully: orderId={}", 
                    order.getOrderId());

            // Acknowledge the message after successful processing
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Notification Service - Error sending email after all retries: orderId={}", 
                    order.getOrderId(), e);
            // Don't acknowledge - CustomErrorHandler will send to DLQ
            throw e;
        }
    }

    private void sendEmail(OrderDto order) {
        // Simulate email sending
        String emailContent = String.format(
                "Dear Customer,\n\n" +
                "Your order has been confirmed!\n\n" +
                "Order ID: %s\n" +
                "Total Amount: %s\n" +
                "Items: %d\n\n" +
                "Thank you for your purchase!",
                order.getOrderId(),
                order.getTotalAmount(),
                order.getItems().size()
        );

        log.info("Email sent to user {}:\n{}", order.getUserId(), emailContent);
    }

    /**
     * Send email with possibility of throwing exception for retry demonstration
     * 20% chance of throwing exception to demonstrate retry mechanism
     */
    private void sendEmailWithRetry(OrderDto order) throws Exception {
        // Simulate transient errors (SMTP server timeout, network issues, etc.)
        if (Math.random() < 0.2) {
            throw new RuntimeException("Transient error: SMTP server timeout");
        }
        
        // Simulate email sending
        String emailContent = String.format(
                "Dear Customer,\n\n" +
                "Your order has been confirmed!\n\n" +
                "Order ID: %s\n" +
                "Total Amount: %s\n" +
                "Items: %d\n\n" +
                "Thank you for your purchase!",
                order.getOrderId(),
                order.getTotalAmount(),
                order.getItems().size()
        );

        log.info("Email sent to user {}:\n{}", order.getUserId(), emailContent);
    }
}
