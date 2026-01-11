# 🚀 Lộ Trình Học Kafka Từ Cơ Bản Đến Nâng Cao

## 📋 Mục Lục
1. [Level 1: Kiến Thức Nền Tảng](#level-1-kiến-thức-nền-tảng)
2. [Level 2: Bài Toán Thực Tế Cơ Bản](#level-2-bài-toán-thực-tế-cơ-bản)
3. [Level 3: Xử Lý Lỗi & Reliability](#level-3-xử-lý-lỗi--reliability)
4. [Level 4: Performance & Optimization](#level-4-performance--optimization)
5. [Level 5: Advanced Patterns](#level-5-advanced-patterns)
6. [Level 6: Production Ready](#level-6-production-ready)

---

## Level 1: Kiến Thức Nền Tảng

### 1.1 Khái Niệm Cơ Bản
- ✅ **Topic, Partition, Offset**: Đã làm trong project hiện tại
- ✅ **Producer, Consumer, Broker**: Đã setup
- 🔄 **Consumer Group**: Cần thực hành thêm

### 1.2 Bài Tập Thực Hành
1. **Understanding Partitions**: 
   - Tạo topic với số partition khác nhau (1, 3, 6)
   - Gửi message với key và không có key
   - Quan sát message được phân bổ như thế nào

2. **Consumer Groups**:
   - Tạo nhiều consumer trong cùng một group
   - Quan sát load balancing
   - Tạo nhiều consumer group khác nhau cùng consume một topic

---

## Level 2: Bài Toán Thực Tế Cơ Bản

### 🛒 Bài Toán 1: Hệ Thống E-Commerce - Order Processing

**Yêu cầu**:
- User đặt hàng → Producer gửi order event
- Order Service xử lý order (tạo order trong DB)
- Inventory Service cập nhật số lượng hàng
- Notification Service gửi email xác nhận
- Payment Service xử lý thanh toán

**Kafka Topics**:
```
- order.created
- order.processed
- order.paid
- inventory.updated
- notification.sent
```

**Key Concepts Học**:
- Multiple consumers từ cùng topic
- Event-driven architecture
- Topic naming conventions

### 📊 Bài Toán 2: Real-time Analytics - User Activity Tracking

**Yêu cầu**:
- Track user clicks, views, purchases
- Aggregate data theo thời gian thực
- Dashboard hiển thị metrics

**Kafka Topics**:
```
- user.click
- user.view
- user.purchase
- analytics.pageview
```

**Key Concepts Học**:
- High throughput producers
- Consumer batch processing
- Time-windowed aggregation

### 📱 Bài Toán 3: Notification System

**Yêu cầu**:
- Gửi notification qua nhiều kênh (Email, SMS, Push)
- Retry logic khi gửi thất bại
- Priority queue cho notification quan trọng

**Kafka Topics**:
```
- notification.high-priority
- notification.normal-priority
- notification.email
- notification.sms
- notification.push
```

**Key Concepts Học**:
- Message priority
- Error handling
- Dead letter topic

---

## Level 3: Xử Lý Lỗi & Reliability

### 3.1 Producer Reliability
- ✅ **Idempotent Producer**: Tránh duplicate messages
- ✅ **Transactions**: Atomic writes
- ✅ **Acknowledgment modes**: acks=0, 1, all

**Thực hành**:
```java
// Enable idempotence
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

// Transactional producer
kafkaTemplate.executeInTransaction(operations -> {
    operations.send("topic1", "message1");
    operations.send("topic2", "message2");
    return null;
});
```

### 3.2 Consumer Reliability
- ✅ **At-least-once vs At-most-once vs Exactly-once**
- ✅ **Commit strategies**: Auto vs Manual commit
- ✅ **Error handling**: Retry, Dead Letter Queue

**Thực hành**:
```java
// Manual commit
@KafkaListener(topics = "orders")
public void consume(ConsumerRecord<String, String> record,
                   Acknowledgment ack) {
    try {
        processOrder(record);
        ack.acknowledge(); // Commit sau khi xử lý thành công
    } catch (Exception e) {
        // Gửi vào dead letter topic
    }
}
```

### 3.3 Dead Letter Queue Pattern
- Xử lý message bị lỗi sau nhiều lần retry
- Audit và analyze failed messages

---

## Level 4: Performance & Optimization

### 4.1 Producer Optimization
- **Batching**: Giảm số lượng request
- **Compression**: Giảm network bandwidth
- **Async sending**: Non-blocking

```java
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
```

### 4.2 Consumer Optimization
- **Concurrency**: Xử lý song song nhiều partition
- **Batch processing**: Xử lý nhiều message cùng lúc
- **Consumer groups tuning**

```java
@KafkaListener(topics = "orders", 
               concurrency = "3") // 3 threads
public void consume(List<ConsumerRecord<String, String>> records) {
    // Batch processing
}
```

### 4.3 Partitioning Strategy
- Cách chọn key để phân bổ message đồng đều
- Custom partitioner

---

## Level 5: Advanced Patterns

### 5.1 Kafka Streams
**Bài toán**: Real-time data transformation

**Ví dụ**: Tính tổng doanh thu theo từng phút
```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, Order> orders = builder.stream("orders");

orders
    .groupByKey()
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(() -> 0.0, (key, order, total) -> total + order.getAmount())
    .toStream()
    .to("revenue-per-minute");
```

### 5.2 Event Sourcing
- Store tất cả events thay vì chỉ current state
- Rebuild state từ events

### 5.3 CQRS (Command Query Responsibility Segregation)
- Separate read và write models
- Kafka làm event bus

### 5.4 Saga Pattern
- Distributed transactions
- Compensating transactions

**Ví dụ**: Order → Payment → Inventory → Shipping
```
order.created → payment.processed → inventory.reserved → shipping.scheduled
                ↓ nếu lỗi
              payment.refunded ← order.cancelled
```

---

## Level 6: Production Ready

### 6.1 Monitoring & Observability
- **Metrics**: Lag, throughput, error rate
- **Logging**: Structured logs
- **Tracing**: Distributed tracing

### 6.2 Schema Registry (Confluent Schema Registry / Apicurio)
- Manage message schemas
- Versioning
- Compatibility checks

```java
// Using Avro
ProducerRecord<String, User> record = 
    new ProducerRecord<>("users", userId, user);
```

### 6.3 Security
- **SSL/TLS**: Encrypted communication
- **SASL**: Authentication
- **ACLs**: Authorization

### 6.4 Multi-Datacenter
- Replication
- MirrorMaker 2.0
- Disaster recovery

---

## 📚 Tài Nguyên Học Tập

### Books
1. **"Kafka: The Definitive Guide"** - Neha Narkhede
2. **"Designing Event-Driven Systems"** - Ben Stopford

### Online Courses
1. Confluent Developer Courses (free)
2. Udemy: Apache Kafka Series

### Practice Projects
1. ✅ E-Commerce Order Processing System
2. ✅ Real-time Analytics Dashboard
3. ✅ Microservices Communication
4. ✅ Event Sourcing Application
5. ✅ Log Aggregation System

---

## 🎯 Checklist Tiến Độ

### Level 1 - Foundation
- [ ] Hiểu rõ Producer/Consumer/Partition
- [ ] Setup Kafka cluster local
- [ ] Gửi/nhận message đơn giản
- [ ] Hiểu Consumer Groups

### Level 2 - Real-world Basics
- [ ] Build một hệ thống event-driven đơn giản
- [ ] Multiple services communicate qua Kafka
- [ ] Implement dead letter queue
- [ ] Handle errors properly

### Level 3 - Reliability
- [ ] Idempotent producer
- [ ] Exactly-once semantics
- [ ] Manual commit strategy
- [ ] Retry mechanisms

### Level 4 - Performance
- [ ] Optimize producer (batching, compression)
- [ ] Optimize consumer (concurrency)
- [ ] Monitor lag và throughput
- [ ] Tune consumer groups

### Level 5 - Advanced
- [ ] Kafka Streams application
- [ ] Event sourcing pattern
- [ ] CQRS implementation
- [ ] Saga pattern

### Level 6 - Production
- [ ] Setup monitoring (Prometheus/Grafana)
- [ ] Schema Registry
- [ ] Security (SSL, SASL)
- [ ] Disaster recovery plan

---

## 💡 Tips Học Tập

1. **Học bằng cách làm**: Code nhiều hơn đọc
2. **Thực hành với bài toán thực tế**: E-commerce, Analytics, Notifications
3. **Quan sát metrics**: Luôn monitor lag, throughput
4. **Đọc Kafka logs**: Hiểu được Kafka đang làm gì
5. **Tham gia community**: Stack Overflow, Reddit r/apachekafka
6. **Experiment**: Thử nhiều config khác nhau, xem kết quả

---

## 🔄 Next Steps

Bạn muốn bắt đầu với bài toán nào?
1. 🛒 **E-Commerce Order System** - Tốt cho beginner
2. 📊 **Real-time Analytics** - Tốt cho hiểu performance
3. 📱 **Notification System** - Tốt cho hiểu error handling
4. 🎯 **Custom bài toán của bạn** - Theo yêu cầu cụ thể
