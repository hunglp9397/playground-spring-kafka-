package com.hunglp.producer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
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

    public UserPurchaseEvent(String userId, String sessionId, String orderId,
                           List<PurchaseItem> items, BigDecimal totalAmount,
                           String paymentMethod, String currency) {
        super(userId, sessionId, LocalDateTime.now(), "PURCHASE");
        this.orderId = orderId;
        this.items = items;
        this.totalAmount = totalAmount;
        this.paymentMethod = paymentMethod;
        this.currency = currency;
    }

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
