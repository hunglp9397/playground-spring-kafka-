package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.mapper.OrderMapper;
import com.hunglp.consumer.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final KafkaTemplate<String, OrderDto> orderKafkaTemplate;
    private final RetryTemplate retryTemplate;
    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;

    @KafkaListener(
            topics = "order.created",
            groupId = "payment-service-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void processPayment(@Payload OrderDto order,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset,
                             @Header(KafkaHeaders.RECEIVED_KEY) String key,
                             Acknowledgment acknowledgment) {
        try {
            log.info("Payment Service - Processing payment: orderId={}, amount={}, partition={}, offset={}",
                    order.getOrderId(), order.getTotalAmount(), partition, offset);

            // Use RetryTemplate for retry with exponential backoff
            retryTemplate.execute(context -> {
                // Save or update order in database
                saveOrUpdateOrder(order);

                // Simulate payment processing that might fail with transient errors
                boolean paymentSuccessful = processPaymentWithRetry(order);

                if (paymentSuccessful) {
                    order.setStatus("PAID");
                    updateOrderStatus(order.getOrderId(), "PAID");
                    log.info("Payment Service - Payment processed successfully: orderId={}", order.getOrderId());

                    // Send to order.paid topic
                    orderKafkaTemplate.send("order.paid", order.getOrderId(), order)
                            .whenComplete((result, exception) -> {
                                if (exception == null) {
                                    log.info("Payment Service - Order paid event sent: orderId={}, partition={}, offset={}",
                                            order.getOrderId(),
                                            result.getRecordMetadata().partition(),
                                            result.getRecordMetadata().offset());
                                } else {
                                    log.error("Payment Service - Failed to send order paid event: orderId={}",
                                            order.getOrderId(), exception);
                                }
                            });
                } else {
                    order.setStatus("FAILED");
                    updateOrderStatus(order.getOrderId(), "FAILED");
                    log.warn("Payment Service - Payment failed: orderId={}", order.getOrderId());

                    // Send to order.cancelled topic
                    orderKafkaTemplate.send("order.cancelled", order.getOrderId(), order);
                }
                return null;
            });

            // Acknowledge the message after successful processing
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Payment Service - Error processing payment after all retries: orderId={}", 
                    order.getOrderId(), e);
            // Don't acknowledge - CustomErrorHandler will send to DLQ
            throw e;
        }
    }

    private boolean processPaymentInternal(OrderDto order) {
        // Simulate payment processing - 85% success rate for demo
        boolean success = ThreadLocalRandom.current().nextDouble() > 0.15;
        log.debug("Payment processing result for order {}: {}", order.getOrderId(), success);
        
        // Simulate payment processing delay
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return success;
    }

    /**
     * Process payment with possibility of throwing exception for retry demonstration
     * 20% chance of throwing exception to demonstrate retry mechanism
     */
    private boolean processPaymentWithRetry(OrderDto order) throws Exception {
        // Simulate transient errors (payment gateway timeout, network issues, etc.)
        if (ThreadLocalRandom.current().nextDouble() < 0.2) {
            throw new RuntimeException("Transient error: Payment gateway timeout");
        }
        
        // Simulate payment processing - 85% success rate for demo
        boolean success = ThreadLocalRandom.current().nextDouble() > 0.15;
        log.debug("Payment processing result for order {}: {}", order.getOrderId(), success);
        
        // Simulate payment processing delay
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return success;
    }

    @Transactional
    private void saveOrUpdateOrder(OrderDto orderDto) {
        com.hunglp.consumer.entity.Order order = orderRepository.findByOrderId(orderDto.getOrderId())
                .orElseGet(() -> orderMapper.toEntity(orderDto));
        
        // Update status if changed
        if (!order.getStatus().equals(orderDto.getStatus())) {
            order.setStatus(orderDto.getStatus());
        }
        
        orderRepository.save(order);
        log.debug("Order saved/updated in database: orderId={}, status={}", 
                order.getOrderId(), order.getStatus());
    }

    @Transactional
    private void updateOrderStatus(String orderId, String status) {
        orderRepository.findByOrderId(orderId).ifPresent(order -> {
            order.setStatus(status);
            orderRepository.save(order);
            log.debug("Order status updated in database: orderId={}, status={}", orderId, status);
        });
    }
}
