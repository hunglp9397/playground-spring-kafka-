package com.hunglp.producer.service;

import com.hunglp.producer.dto.CreateOrderRequest;
import com.hunglp.producer.dto.OrderDto;
import com.hunglp.producer.entity.Order;
import com.hunglp.producer.mapper.OrderMapper;
import com.hunglp.producer.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {

    private final KafkaTemplate<String, OrderDto> orderKafkaTemplate;
    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;

    @Transactional
    public OrderDto createOrder(CreateOrderRequest request) {
        // Generate order ID
        String orderId = UUID.randomUUID().toString();
        
        // Calculate total amount
        BigDecimal totalAmount = request.getItems().stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // Create order DTO
        OrderDto orderDto = orderMapper.toDto(request, orderId, totalAmount);

        // Convert to entity and save to database
        Order orderEntity = orderMapper.toEntity(orderDto);
        orderEntity = orderRepository.save(orderEntity);
        log.info("Order saved to database: orderId={}, userId={}, totalAmount={}", 
                orderId, request.getUserId(), totalAmount);

        // Send order.created event to Kafka
        // Use orderId as key to ensure messages with same order are in same partition
        orderKafkaTemplate.send("order.created", orderId, orderDto)
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        log.info("Order created event sent successfully: orderId={}, partition={}, offset={}",
                                orderId,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to send order created event: orderId={}", orderId, exception);
                    }
                });

        return orderDto;
    }

    public Optional<OrderDto> getOrder(String orderId) {
        return orderRepository.findByOrderId(orderId)
                .map(orderMapper::toDto);
    }

    public List<OrderDto> getOrdersByUser(String userId) {
        return orderRepository.findByUserId(userId).stream()
                .map(orderMapper::toDto)
                .collect(java.util.stream.Collectors.toList());
    }
}
