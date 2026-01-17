# 🛒 E-Commerce Order Processing System - Implementation Guide

## 📋 Tổng Quan

Hệ thống E-Commerce Order Processing đã được implement với các microservices sau:

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    User (Frontend/API)                       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Order Service (Producer)                       │
│  - REST API: POST /api/v1/orders                           │
│  - Lưu order vào database (H2)                            │
│  - Gửi event: order.created                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
            ┌───────────────────────┐
            │   Kafka Cluster       │
            │  Topic: order.created  │
            │  Partitions: 3         │
            │  Replication: 2        │
            └───────────┬────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Inventory   │ │ Notification │ │   Payment    │
│   Service    │ │   Service    │ │   Service    │
│              │ │              │ │              │
│ Group:       │ │ Group:       │ │ Group:       │
│ inventory-   │ │ notification-│ │ payment-     │
│ service-     │ │ service-     │ │ service-     │
│ group        │ │ group        │ │ group        │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                 │                 │
       │                 │                 │
       ▼                 ▼                 ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ order.       │ │   (Log)      │ │ order.paid   │
│ cancelled    │ │              │ │ order.       │
│              │ │              │ │ cancelled    │
└──────────────┘ └──────────────┘ └──────────────┘
       │                                    │
       └──────────────┬─────────────────────┘
                      ▼
            ┌───────────────────────┐
            │  Order Status         │
            │  Listener             │
            │  (order-status-group) │
            └───────────────────────┘
```

### Event Flow

1. **User tạo order** → Order Service nhận request
2. **Order Service** → Lưu vào database → Gửi `order.created` event
3. **3 Services cùng nhận** `order.created` event (mỗi service có consumer group riêng)
4. **Inventory Service** → Check inventory → Gửi `order.cancelled` nếu fail
5. **Payment Service** → Process payment → Gửi `order.paid` hoặc `order.cancelled`
6. **Notification Service** → Gửi email xác nhận
7. **Order Status Listener** → Xử lý `order.paid` và `order.cancelled` events

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
# Runs on port 9090 (default, can be changed in application.properties)
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

## 🎯 Learning Points - Điểm Học Tập Quan Trọng

### 1. Partitioning Strategy ⭐⭐⭐

**Vấn đề**: Làm sao đảm bảo messages của cùng một order được xử lý theo thứ tự?

**Giải pháp**: Sử dụng `orderId` làm key khi gửi message

```java
// Trong OrderService.java
orderKafkaTemplate.send("order.created", orderId, orderDto);
//                              ↑
//                         orderId làm key
```

**Kết quả**:
- Messages có cùng key → vào cùng partition
- Consumer trong cùng partition xử lý tuần tự
- Đảm bảo ordering cho cùng một order

**Thử nghiệm**:
```bash
# Gửi 10 orders với cùng userId
# Xem logs: messages vào partition nào?
# Messages của cùng order sẽ vào cùng partition
```

### 2. Consumer Groups ⭐⭐⭐

**Vấn đề**: Làm sao để nhiều services cùng nhận tất cả messages?

**Giải pháp**: Mỗi service có consumer group riêng

```
Service              Consumer Group
─────────────────────────────────────
Inventory Service    → inventory-service-group
Notification Service → notification-service-group
Payment Service      → payment-service-group
```

**Behavior**:
- Mỗi group nhận **tất cả** messages từ topic
- Trong cùng group: Load balancing (mỗi consumer nhận một phần)
- Khác group: Mỗi group nhận tất cả (broadcast pattern)

**Thử nghiệm**:
```bash
# List consumer groups
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Xem mỗi group nhận messages như thế nào
```

### 3. Manual Acknowledgment ⭐⭐

**Vấn đề**: Khi nào commit offset? Nếu commit sớm và xử lý lỗi thì sao?

**Giải pháp**: Manual acknowledgment - chỉ commit sau khi xử lý thành công

```java
@KafkaListener(...)
public void processOrder(..., Acknowledgment acknowledgment) {
    try {
        // Xử lý order
        processOrderInternal(order);
        
        // Chỉ commit sau khi thành công
        acknowledgment.acknowledge();
    } catch (Exception e) {
        // Không acknowledge → message sẽ được retry
        throw e;
    }
}
```

**Lợi ích**:
- Control tốt hơn về khi nào commit
- Tránh mất messages khi xử lý lỗi
- Message sẽ được retry nếu không acknowledge

### 4. Dead Letter Queue ⭐⭐

**Vấn đề**: Message bị lỗi sau nhiều lần retry thì làm gì?

**Giải pháp**: Gửi vào Dead Letter Queue để investigate

```
Message → Retry 1 → Retry 2 → Retry 3 → DLQ
```

**Sử dụng**:
- Investigate failed messages
- Manual retry sau khi fix
- Alert operations team

### 5. Idempotent Producer ⭐

**Vấn đề**: Làm sao tránh duplicate messages?

**Giải pháp**: Enable idempotence

```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

**Kết quả**: Producer đảm bảo không gửi duplicate messages

### 6. JSON Serialization ⭐

**Vấn đề**: Làm sao serialize/deserialize complex objects?

**Giải pháp**: Sử dụng JsonSerializer/JsonDeserializer

```java
// Producer
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

// Consumer
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
```

## 💡 Best Practices Đã Áp Dụng

1. ✅ **Use meaningful keys**: orderId để đảm bảo ordering
2. ✅ **Separate consumer groups**: Mỗi service có group riêng
3. ✅ **Manual commit**: Control tốt hơn
4. ✅ **Error handling**: Try-catch và retry
5. ✅ **Dead Letter Queue**: Xử lý failed messages
6. ✅ **Idempotent producer**: Tránh duplicates
7. ✅ **Database persistence**: Lưu state vào database

## 🔍 Next Steps (Advanced)

### Đã Implement ✅

1. ✅ **Retry Mechanism**: [RETRY_MECHANISM.md](./RETRY_MECHANISM.md) - Exponential backoff retry
2. ✅ **Database**: [DATABASE_IMPLEMENTATION.md](./DATABASE_IMPLEMENTATION.md) - Lưu orders vào database
3. ✅ **Saga Pattern**: [SAGA_PATTERN_IMPLEMENTATION.md](./SAGA_PATTERN_IMPLEMENTATION.md) - Distributed transaction handling

### Có Thể Mở Rộng 🚀

4. **Add Monitoring**: Metrics và alerts (Prometheus, Grafana)
5. **Add Testing**: Unit tests và integration tests
6. **Add Schema Registry**: Schema versioning và compatibility
7. **Add Security**: SSL/TLS, SASL authentication
8. **Add Multi-Datacenter**: Replication và disaster recovery

## 📚 Tài Liệu Liên Quan

- **[README.md](./README.md)** - Tổng quan project
- **[QUICK_START.md](./QUICK_START.md)** - Hướng dẫn setup nhanh (5 phút)
- **[LEARNING_GUIDE.md](./LEARNING_GUIDE.md)** - Hướng dẫn học từng bước (8 tuần)
- **[KAFKA_LEARNING_PATH.md](./KAFKA_LEARNING_PATH.md)** - Lộ trình học Kafka
- **[DATABASE_IMPLEMENTATION.md](./DATABASE_IMPLEMENTATION.md)** - Database persistence
- **[RETRY_MECHANISM.md](./RETRY_MECHANISM.md)** - Retry mechanism với exponential backoff
- **[SAGA_PATTERN_IMPLEMENTATION.md](./SAGA_PATTERN_IMPLEMENTATION.md)** - Saga Pattern implementation

## 📝 Notes

- Services sử dụng simulation logic (90% success rate cho inventory, 85% cho payment)
- Email sending chỉ log, không thực sự gửi email
- Inventory check không thực sự query database
- Có thể mở rộng với real database, email service, payment gateway

## 📚 Tài Liệu Tham Khảo

- [README.md](./README.md) - Tổng quan project
- [QUICK_START.md](./QUICK_START.md) - Hướng dẫn setup nhanh
- [LEARNING_GUIDE.md](./LEARNING_GUIDE.md) - Hướng dẫn học từng bước
- [DATABASE_IMPLEMENTATION.md](./DATABASE_IMPLEMENTATION.md) - Database persistence
- [RETRY_MECHANISM.md](./RETRY_MECHANISM.md) - Retry mechanism
- [SAGA_PATTERN_IMPLEMENTATION.md](./SAGA_PATTERN_IMPLEMENTATION.md) - Saga Pattern
