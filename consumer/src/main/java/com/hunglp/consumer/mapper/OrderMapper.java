package com.hunglp.consumer.mapper;

import com.hunglp.consumer.dto.OrderDto;
import com.hunglp.consumer.dto.OrderItem;
import com.hunglp.consumer.entity.Order;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Component
public class OrderMapper {

    public Order toEntity(OrderDto dto) {
        Order order = new Order();
        order.setOrderId(dto.getOrderId());
        order.setUserId(dto.getUserId());
        order.setTotalAmount(dto.getTotalAmount());
        order.setStatus(dto.getStatus());
        order.setCreatedAt(dto.getCreatedAt());

        // Map items
        if (dto.getItems() != null) {
            dto.getItems().forEach(itemDto -> {
                com.hunglp.consumer.entity.OrderItem item = new com.hunglp.consumer.entity.OrderItem();
                item.setProductId(itemDto.getProductId());
                item.setProductName(itemDto.getProductName());
                item.setQuantity(itemDto.getQuantity());
                item.setPrice(itemDto.getPrice());
                order.addItem(item);
            });
        }

        return order;
    }

    public OrderDto toDto(Order entity) {
        OrderDto dto = new OrderDto();
        dto.setOrderId(entity.getOrderId());
        dto.setUserId(entity.getUserId());
        dto.setTotalAmount(entity.getTotalAmount());
        dto.setStatus(entity.getStatus());
        dto.setCreatedAt(entity.getCreatedAt());

        // Map items
        if (entity.getItems() != null) {
            dto.setItems(entity.getItems().stream()
                    .map(item -> {
                        OrderItem itemDto = new OrderItem();
                        itemDto.setProductId(item.getProductId());
                        itemDto.setProductName(item.getProductName());
                        itemDto.setQuantity(item.getQuantity());
                        itemDto.setPrice(item.getPrice());
                        return itemDto;
                    })
                    .collect(Collectors.toList()));
        }

        return dto;
    }
}
