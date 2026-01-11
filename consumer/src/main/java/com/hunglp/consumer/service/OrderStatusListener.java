package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.mapper.OrderMapper;
import com.hunglp.consumer.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderStatusListener {

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;

    @KafkaListener(
            topics = "order.paid",
            groupId = "order-status-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void handleOrderPaid(@Payload OrderDto order,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               @Header(KafkaHeaders.OFFSET) long offset,
                               Acknowledgment acknowledgment) {
        try {
            log.info("Order Status Listener - Order paid: orderId={}, amount={}, partition={}, offset={}",
                    order.getOrderId(), order.getTotalAmount(), partition, offset);

            // Update order status in database
            updateOrderStatus(order.getOrderId(), "PAID");
            
            // In real application, trigger shipping process here

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Order Status Listener - Error handling paid order: orderId={}",
                    order.getOrderId(), e);
            throw e;
        }
    }

    @KafkaListener(
            topics = "order.cancelled",
            groupId = "order-status-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void handleOrderCancelled(@Payload OrderDto order,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset,
                                   Acknowledgment acknowledgment) {
        try {
            log.info("Order Status Listener - Order cancelled: orderId={}, reason={}, partition={}, offset={}",
                    order.getOrderId(), order.getStatus(), partition, offset);

            // Update order status in database
            updateOrderStatus(order.getOrderId(), "CANCELLED");
            
            // In real application, send cancellation notification here

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Order Status Listener - Error handling cancelled order: orderId={}",
                    order.getOrderId(), e);
            throw e;
        }
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
