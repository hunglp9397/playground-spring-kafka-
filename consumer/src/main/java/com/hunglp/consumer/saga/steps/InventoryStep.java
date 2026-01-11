package com.hunglp.consumer.saga.steps;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.saga.SagaState;
import com.hunglp.consumer.saga.SagaStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Inventory Step: Check and reserve inventory
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class InventoryStep implements SagaStep {

    @Override
    public boolean execute(OrderDto order) {
        log.info("Saga - Inventory Step: Executing for orderId={}", order.getOrderId());
        
        // Simulate inventory check and reservation
        // In real application, this would:
        // 1. Check product availability in database
        // 2. Reserve inventory
        // 3. Update inventory records
        
        boolean available = checkInventory(order);
        
        if (available) {
            reserveInventory(order);
            log.info("Saga - Inventory Step: Successfully reserved inventory for orderId={}", order.getOrderId());
            return true;
        } else {
            log.warn("Saga - Inventory Step: Insufficient inventory for orderId={}", order.getOrderId());
            return false;
        }
    }

    @Override
    public boolean compensate(OrderDto order) {
        log.info("Saga - Inventory Step: Compensating (releasing inventory) for orderId={}", order.getOrderId());
        
        // Release reserved inventory
        // In real application, this would:
        // 1. Release reserved inventory
        // 2. Update inventory records
        
        order.getItems().forEach(item -> {
            log.debug("Saga - Inventory Step: Releasing inventory - ProductId: {}, Quantity: {}", 
                    item.getProductId(), item.getQuantity());
        });
        
        log.info("Saga - Inventory Step: Compensation completed for orderId={}", order.getOrderId());
        return true;
    }

    @Override
    public SagaState getSuccessState() {
        return SagaState.INVENTORY_RESERVED;
    }

    @Override
    public String getStepName() {
        return "INVENTORY_STEP";
    }

    private boolean checkInventory(OrderDto order) {
        // Simulate inventory check - 90% success rate
        boolean available = ThreadLocalRandom.current().nextDouble() > 0.1;
        log.debug("Saga - Inventory check result for order {}: {}", order.getOrderId(), available);
        return available;
    }

    private void reserveInventory(OrderDto order) {
        // Simulate inventory reservation
        order.getItems().forEach(item -> {
            log.debug("Saga - Reserving inventory - ProductId: {}, Quantity: {}", 
                    item.getProductId(), item.getQuantity());
        });
    }
}
