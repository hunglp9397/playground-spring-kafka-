package com.hunglp.consumer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class UserPurchaseEvent extends UserEvent {
    private String orderId;
    private List<PurchaseItem> items;
    private BigDecimal totalAmount;
    private String paymentMethod;
    private String currency;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PurchaseItem {
        private String productId;
        private String productName;
        private Integer quantity;
        private BigDecimal price;
    }
}
