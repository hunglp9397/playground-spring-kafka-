# 🛒 Ví Dụ Thực Tế: E-Commerce Order Processing System

## Mô Tả Hệ Thống

Một hệ thống xử lý đơn hàng đơn giản với các microservices:
1. **Order Service** (Producer): Nhận đơn hàng từ user
2. **Inventory Service** (Consumer): Cập nhật tồn kho
3. **Notification Service** (Consumer): Gửi email xác nhận
4. **Payment Service** (Consumer): Xử lý thanh toán

## Kiến Trúc

```
User → Order Service → Kafka Topic: "order.created"
                              ↓
              ┌───────────────┼───────────────┐
              ↓               ↓               ↓
      Inventory Service  Notification   Payment Service
      (order.created)    (order.created)  (order.created)
              ↓               ↓               ↓
      Inventory Updated   Email Sent    Payment Processed
              ↓
      Kafka: "inventory.updated"
              ↓
      Analytics Service (optional)
```

## Topics Cần Tạo

```bash
# Tạo topics
kafka-topics --create --topic order.created --partitions 3 --replication-factor 2
kafka-topics --create --topic order.paid --partitions 3 --replication-factor 2
kafka-topics --create --topic order.cancelled --partitions 3 --replication-factor 2
kafka-topics --create --topic inventory.updated --partitions 3 --replication-factor 2
kafka-topics --create --topic notification.sent --partitions 3 --replication-factor 2
kafka-topics --create --topic dlq.order.failed --partitions 1 --replication-factor 2
```

## DTOs Cần Tạo

### OrderDto
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderDto {
    private String orderId;
    private String userId;
    private List<OrderItem> items;
    private BigDecimal totalAmount;
    private String status; // CREATED, PAID, CANCELLED
    private LocalDateTime createdAt;
}
```

### OrderItem
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderItem {
    private String productId;
    private Integer quantity;
    private BigDecimal price;
}
```

## Implementation Steps

### Step 1: Order Service (Producer)
- REST API nhận order từ user
- Gửi event `order.created` vào Kafka
- Handle errors và retries

### Step 2: Inventory Service (Consumer)
- Consume `order.created` events
- Kiểm tra và cập nhật inventory
- Gửi event `inventory.updated` hoặc `inventory.failed`

### Step 3: Notification Service (Consumer)
- Consume `order.created` events
- Gửi email xác nhận
- Gửi event `notification.sent`

### Step 4: Payment Service (Consumer)
- Consume `order.created` events
- Xử lý thanh toán
- Gửi event `order.paid` hoặc `order.failed`

### Step 5: Error Handling
- Dead Letter Queue cho messages bị lỗi
- Retry mechanism
- Monitoring và alerting

## Testing Scenarios

1. **Happy Path**: Order → Inventory → Notification → Payment → Success
2. **Inventory Out of Stock**: Order → Inventory check fails → Cancel order
3. **Payment Fails**: Order → Payment fails → Retry → Success
4. **Consumer Crashes**: Order → Consumer crashes → Restart → Continue processing
5. **High Load**: 1000 orders/second → All services handle correctly

## Key Learning Points

1. ✅ Multiple consumers từ cùng topic
2. ✅ Event-driven architecture
3. ✅ Error handling và Dead Letter Queue
4. ✅ Idempotency (tránh xử lý duplicate orders)
5. ✅ Monitoring consumer lag
6. ✅ Ordering guarantees (process order theo thứ tự)
