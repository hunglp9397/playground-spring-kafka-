package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.entity.Order;
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
public class InventoryService {

    private final KafkaTemplate<String, OrderDto> orderKafkaTemplate;
    private final RetryTemplate retryTemplate;
    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;

    @KafkaListener(
            topics = "order.created",
            groupId = "inventory-service-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void processOrder(@Payload OrderDto order,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                           @Header(KafkaHeaders.OFFSET) long offset,
                           @Header(KafkaHeaders.RECEIVED_KEY) String key,
                           Acknowledgment acknowledgment) {
        try {
            log.info("Inventory Service - Processing order: orderId={}, partition={}, offset={}, key={}",
                    order.getOrderId(), partition, offset, key);

            // Use RetryTemplate for retry with exponential backoff
            retryTemplate.execute(context -> {
                // Save or update order in database
                saveOrUpdateOrder(order);

                // Simulate inventory check that might fail
                // In real application, this would check database for product availability
                boolean inventoryAvailable = checkInventoryWithRetry(order);

                if (inventoryAvailable) {
                    // Reserve inventory
                    reserveInventory(order);
                    log.info("Inventory Service - Inventory reserved successfully: orderId={}", order.getOrderId());
                } else {
                    // Cancel order due to insufficient inventory
                    order.setStatus("CANCELLED");
                    updateOrderStatus(order.getOrderId(), "CANCELLED");
                    log.warn("Inventory Service - Insufficient inventory, cancelling order: orderId={}", 
                            order.getOrderId());
                    
                    // Send to order.cancelled topic
                    orderKafkaTemplate.send("order.cancelled", order.getOrderId(), order);
                }
                return null;
            });

            // Acknowledge the message after successful processing
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Inventory Service - Error processing order after all retries: orderId={}", 
                    order.getOrderId(), e);
            // Don't acknowledge - CustomErrorHandler will send to DLQ
            throw e;
        }
    }

    private boolean checkInventory(OrderDto order) {
        // Simulate inventory check - 90% success rate for demo
        boolean available = ThreadLocalRandom.current().nextDouble() > 0.1;
        log.debug("Inventory check result for order {}: {}", order.getOrderId(), available);
        return available;
    }

    /**
     * Check inventory with possibility of throwing exception for retry demonstration
     * 20% chance of throwing exception to demonstrate retry mechanism
     */
    private boolean checkInventoryWithRetry(OrderDto order) throws Exception {
        // Simulate transient errors (network issues, database timeout, etc.)
        if (ThreadLocalRandom.current().nextDouble() < 0.2) {
            throw new RuntimeException("Transient error: Database connection timeout");
        }
        
        // Simulate inventory check - 90% success rate for demo
        boolean available = ThreadLocalRandom.current().nextDouble() > 0.1;
        log.debug("Inventory check result for order {}: {}", order.getOrderId(), available);
        return available;
    }

    private void reserveInventory(OrderDto order) {
        // Simulate inventory reservation
        // In real application, this would update database
        order.getItems().forEach(item -> {
            log.debug("Reserving inventory - ProductId: {}, Quantity: {}", 
                    item.getProductId(), item.getQuantity());
        });
    }

    @Transactional
    private void saveOrUpdateOrder(OrderDto orderDto) {
        Order order = orderRepository.findByOrderId(orderDto.getOrderId())
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
