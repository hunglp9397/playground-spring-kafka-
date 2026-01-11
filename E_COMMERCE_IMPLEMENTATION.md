# 🛒 E-Commerce Order Processing System - Implementation Guide

## 📋 Tổng Quan

Hệ thống E-Commerce Order Processing đã được implement với các microservices sau:

### Architecture

```
User Request → Order Service (Producer)
                ↓
        Topic: order.created (3 partitions)
                ↓
    ┌───────────┼───────────┐
    ↓           ↓           ↓
Inventory   Notification  Payment
Service     Service       Service
(group 1)   (group 2)    (group 3)
    ↓           ↓           ↓
order.cancelled  ✓      order.paid
                      order.cancelled
```

## 🚀 Components

### Producer (Order Service)

**Location**: `producer/src/main/java/com/hunglp/producer/`

- **OrderController**: REST API endpoint `/api/v1/orders` để tạo order
- **OrderService**: Business logic để tạo order và gửi event vào Kafka
- **DTOs**:
  - `OrderDto`: Order entity
  - `OrderItem`: Order item entity
  - `CreateOrderRequest`: Request DTO

**API Endpoint**:
```bash
POST http://localhost:8080/api/v1/orders
Content-Type: application/json

{
  "userId": "user123",
  "items": [
    {
      "productId": "prod1",
      "productName": "Laptop",
      "quantity": 1,
      "price": 1000.00
    },
    {
      "productId": "prod2",
      "productName": "Mouse",
      "quantity": 2,
      "price": 25.00
    }
  ]
}
```

### Consumer Services

**Location**: `consumer/src/main/java/com/hunglp/consumer/service/`

#### 1. Inventory Service
- **Group ID**: `inventory-service-group`
- **Topic**: `order.created`
- **Chức năng**: 
  - Kiểm tra tồn kho
  - Reserve inventory nếu có đủ hàng
  - Gửi event `order.cancelled` nếu không đủ hàng

#### 2. Notification Service
- **Group ID**: `notification-service-group`
- **Topic**: `order.created`
- **Chức năng**: Gửi email xác nhận đơn hàng

#### 3. Payment Service
- **Group ID**: `payment-service-group`
- **Topic**: `order.created`
- **Chức năng**:
  - Xử lý thanh toán
  - Gửi event `order.paid` nếu thanh toán thành công
  - Gửi event `order.cancelled` nếu thanh toán thất bại

#### 4. Order Status Listener
- **Group ID**: `order-status-group`
- **Topics**: `order.paid`, `order.cancelled`
- **Chức năng**: Xử lý các event về trạng thái order

#### 5. Dead Letter Queue Service
- **Group ID**: `dlq-processor-group`
- **Topic**: `dlq.order.failed`
- **Chức năng**: Xử lý messages bị lỗi sau nhiều lần retry

## 🔧 Setup & Configuration

### 1. Start Kafka Cluster

```bash
docker-compose up -d
```

### 2. Create Topics

**Windows PowerShell**:
```powershell
.\create-topics.ps1
```

**Linux/Mac**:
```bash
chmod +x create-topics.sh
./create-topics.sh
```

**Manual**:
```bash
docker  exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create \
  --topic order.created \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2

docker  exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create \
  --topic order.paid \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2

docker  exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create \
  --topic order.cancelled \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2

docker  exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create \
  --topic dlq.order.failed \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 2
```

### 3. Start Services

**Producer (Order Service)**:
```bash
cd producer
mvn spring-boot:run
# Runs on port 8080 (default)
```

**Consumer (All Services)**:
```bash
cd consumer
mvn spring-boot:run
# Runs on port 8081 (default, can be changed in application.properties)
```

## 🧪 Testing

### 1. Create Order

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "items": [
      {
        "productId": "prod1",
        "productName": "Laptop",
        "quantity": 1,
        "price": 1000.00
      },
      {
        "productId": "prod2",
        "productName": "Mouse",
        "quantity": 2,
        "price": 25.00
      }
    ]
  }'
```

### 2. Monitor Logs

**Producer logs**:
```bash
# Order created and event sent
```

**Consumer logs**:
```bash
# Inventory Service - Processing order
# Notification Service - Sending email
# Payment Service - Processing payment
# Order Status Listener - Order paid/cancelled
```

### 3. Monitor Kafka Topics

```bash
# Consume from order.created topic
docker exec kafka1 kafka-console-consumer \
  --topic order.created \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Consume from order.paid topic
docker exec kafka1 kafka-console-consumer \
  --topic order.paid \
  --bootstrap-server localhost:9092 \
  --from-beginning

# List consumer groups
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Check consumer lag
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group inventory-service-group \
  --describe
```

## 📊 Key Features Implemented

### ✅ 1. Multiple Consumers from Same Topic
- 3 services (Inventory, Notification, Payment) consume từ `order.created`
- Mỗi service có consumer group riêng → mỗi service nhận được tất cả messages

### ✅ 2. JSON Serialization
- Producer sử dụng `JsonSerializer` cho OrderDto
- Consumer sử dụng `JsonDeserializer` để deserialize OrderDto

### ✅ 3. Idempotent Producer
- Enable `ENABLE_IDEMPOTENCE_CONFIG = true`
- Đảm bảo không duplicate messages

### ✅ 4. Manual Commit
- Consumer sử dụng `MANUAL_IMMEDIATE` acknowledgment
- Chỉ commit sau khi xử lý thành công

### ✅ 5. Concurrency
- Consumer factory có `setConcurrency(3)`
- Xử lý song song nhiều messages

### ✅ 6. Error Handling
- Try-catch trong mỗi consumer
- Không acknowledge nếu xử lý lỗi → message sẽ được retry
- Dead Letter Queue pattern

### ✅ 7. Event-Driven Architecture
- Order Service gửi `order.created` event
- Payment Service gửi `order.paid` hoặc `order.cancelled`
- Inventory Service gửi `order.cancelled` nếu không đủ hàng

## 🎯 Learning Points

### 1. Partitioning Strategy
- Sử dụng `orderId` làm key → messages của cùng order sẽ vào cùng partition
- Đảm bảo ordering cho cùng một order

### 2. Consumer Groups
- Mỗi service có group riêng → mỗi service nhận tất cả messages
- Load balancing trong cùng một group

### 3. Manual Acknowledgment
- Control tốt hơn về khi nào commit offset
- Tránh mất messages khi xử lý lỗi

### 4. Dead Letter Queue
- Xử lý messages bị lỗi sau nhiều lần retry
- Cho phép investigate và retry manually

## 🔍 Next Steps (Advanced)

1. **Add Retry Mechanism**: Implement exponential backoff retry
2. **Add Database**: Lưu orders vào database
3. **Add Monitoring**: Metrics và alerts
4. **Add Testing**: Unit tests và integration tests
5. **Add Saga Pattern**: Distributed transaction handling
6. **Add Schema Registry**: Schema versioning và compatibility

## 📝 Notes

- Services sử dụng simulation logic (90% success rate cho inventory, 85% cho payment)
- Email sending chỉ log, không thực sự gửi email
- Inventory check không thực sự query database
- Có thể mở rộng với real database, email service, payment gateway
