# 📚 Learning Guide - Hướng Dẫn Học Từng Bước

Hướng dẫn chi tiết để học Kafka và Event-Driven Architecture thông qua project này.

## 🎯 Mục Tiêu

Sau khi hoàn thành guide này, bạn sẽ:
- ✅ Hiểu cách Kafka hoạt động trong microservices
- ✅ Biết cách implement Event-Driven Architecture
- ✅ Hiểu các Kafka patterns và best practices
- ✅ Có thể tự build một hệ thống tương tự

## 📖 Lộ Trình Học (8 Tuần)

### Tuần 1: Làm Quen Với Project

#### Mục Tiêu
- Setup và chạy được project
- Hiểu flow cơ bản: Producer → Kafka → Consumer

#### Nhiệm Vụ

1. **Setup Project** (30 phút)
   - Đọc [QUICK_START.md](./QUICK_START.md)
   - Start Kafka cluster
   - Tạo topics
   - Start services

2. **Test Cơ Bản** (30 phút)
   - Tạo một order qua API
   - Xem logs của producer và consumer
   - Hiểu message flow

3. **Khám Phá Code** (1 giờ)
   - Đọc `OrderController.java` - API endpoint
   - Đọc `OrderService.java` - Business logic
   - Đọc `KafkaProducerConfig.java` - Kafka config
   - Đọc một consumer service (ví dụ: `InventoryService.java`)

#### Câu Hỏi Tự Kiểm Tra

- [ ] Message được gửi từ đâu đến đâu?
- [ ] Consumer nhận message như thế nào?
- [ ] Tại sao có nhiều consumer services?

#### Tài Liệu Tham Khảo
- [QUICK_START.md](./QUICK_START.md)
- [E_COMMERCE_IMPLEMENTATION.md](./E_COMMERCE_IMPLEMENTATION.md) - Phần "Components"

---

### Tuần 2: Hiểu Kafka Fundamentals

#### Mục Tiêu
- Hiểu Topics, Partitions, Offsets
- Hiểu Consumer Groups
- Hiểu Serialization/Deserialization

#### Nhiệm Vụ

1. **Topics và Partitions** (1 giờ)
   ```bash
   # List topics
   docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
   
   # Describe topic
   docker exec kafka1 kafka-topics --describe \
     --topic order.created \
     --bootstrap-server localhost:9092
   ```
   - Hiểu: Topic có bao nhiêu partitions?
   - Tại sao dùng 3 partitions?

2. **Consumer Groups** (1 giờ)
   ```bash
   # List consumer groups
   docker exec kafka1 kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --list
   
   # Describe group
   docker exec kafka1 kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group inventory-service-group \
     --describe
   ```
   - Hiểu: Tại sao mỗi service có group riêng?
   - Xem consumer lag

3. **Message Flow** (1 giờ)
   - Tạo 10 orders
   - Xem messages được phân bổ như thế nào
   - Check logs: messages vào partition nào?

#### Thực Hành

**Exercise 1**: Tạo topic mới với 1 partition, gửi messages, xem behavior khác gì?

**Exercise 2**: Tạo 2 consumers trong cùng group, xem load balancing

#### Câu Hỏi Tự Kiểm Tra

- [ ] Partition là gì? Tại sao cần partitions?
- [ ] Consumer Group là gì? Tác dụng?
- [ ] Key trong message có tác dụng gì?

#### Tài Liệu Tham Khảo
- [KAFKA_LEARNING_PATH.md](./KAFKA_LEARNING_PATH.md) - Level 1
- [E_COMMERCE_IMPLEMENTATION.md](./E_COMMERCE_IMPLEMENTATION.md) - "Learning Points"

---

### Tuần 3: Event-Driven Architecture

#### Mục Tiêu
- Hiểu Event-Driven Architecture
- Hiểu cách services communicate qua events
- Hiểu event flow

#### Nhiệm Vụ

1. **Event Flow Analysis** (1 giờ)
   - Vẽ diagram event flow
   - Trace một order từ đầu đến cuối
   - Xem các events được tạo ra

2. **Multiple Consumers** (1 giờ)
   - Hiểu tại sao 3 services cùng consume `order.created`
   - Xem logs: mỗi service nhận message như thế nào
   - Test: Tắt một service, xem behavior

3. **Event Types** (1 giờ)
   - `order.created` - Khi nào được tạo?
   - `order.paid` - Khi nào được tạo?
   - `order.cancelled` - Khi nào được tạo?

#### Thực Hành

**Exercise**: Thêm một service mới (ví dụ: ShippingService) consume `order.paid`

#### Câu Hỏi Tự Kiểm Tra

- [ ] Event-Driven Architecture khác gì với Request-Response?
- [ ] Tại sao dùng events thay vì direct calls?
- [ ] Lợi ích của Event-Driven Architecture?

#### Tài Liệu Tham Khảo
- [E_COMMERCE_IMPLEMENTATION.md](./E_COMMERCE_IMPLEMENTATION.md) - "Event-Driven Architecture"
- [PROJECT_EXAMPLES.md](./PROJECT_EXAMPLES.md)

---

### Tuần 4: Error Handling & Reliability

#### Mục Tiêu
- Hiểu error handling trong Kafka
- Hiểu retry mechanism
- Hiểu Dead Letter Queue

#### Nhiệm Vụ

1. **Error Handling** (1 giờ)
   - Đọc [RETRY_MECHANISM.md](./RETRY_MECHANISM.md)
   - Xem code: `CustomErrorHandler.java`
   - Test: Tạo order, xem error handling

2. **Retry Mechanism** (1 giờ)
   - Đọc `RetryConfig.java`
   - Hiểu exponential backoff
   - Test: Simulate errors, xem retry behavior

3. **Dead Letter Queue** (1 giờ)
   - Xem code: `DeadLetterQueueService.java`
   - Test: Tạo message fail, xem DLQ
   - Query DLQ topic

#### Thực Hành

**Exercise**: Tùy chỉnh retry config (số lần retry, delay)

#### Câu Hỏi Tự Kiểm Tra

- [ ] Tại sao cần retry mechanism?
- [ ] Exponential backoff là gì?
- [ ] Dead Letter Queue dùng để làm gì?

#### Tài Liệu Tham Khảo
- [RETRY_MECHANISM.md](./RETRY_MECHANISM.md)

---

### Tuần 5: Database Persistence

#### Mục Tiêu
- Hiểu cách lưu data trong event-driven system
- Hiểu Entity-DTO mapping
- Hiểu transaction management

#### Nhiệm Vụ

1. **Database Setup** (30 phút)
   - Đọc [DATABASE_IMPLEMENTATION.md](./DATABASE_IMPLEMENTATION.md)
   - Access H2 console
   - Query orders

2. **Entity Mapping** (1 giờ)
   - Xem `Order.java` entity
   - Xem `OrderMapper.java`
   - Hiểu Entity vs DTO

3. **Transaction Management** (1 giờ)
   - Xem `@Transactional` annotations
   - Test: Tạo order, check database
   - Test: Consumer update order status

#### Thực Hành

**Exercise**: Thêm field mới vào Order entity, update mapper

#### Câu Hỏi Tự Kiểm Tra

- [ ] Tại sao cần Entity và DTO riêng?
- [ ] Transaction trong event-driven system khác gì?
- [ ] Khi nào cần transaction?

#### Tài Liệu Tham Khảo
- [DATABASE_IMPLEMENTATION.md](./DATABASE_IMPLEMENTATION.md)

---

### Tuần 6: Saga Pattern

#### Mục Tiêu
- Hiểu distributed transactions
- Hiểu Saga Pattern
- Hiểu compensation

#### Nhiệm Vụ

1. **Saga Pattern Overview** (1 giờ)
   - Đọc [SAGA_PATTERN_IMPLEMENTATION.md](./SAGA_PATTERN_IMPLEMENTATION.md)
   - Hiểu vấn đề distributed transactions
   - Hiểu giải pháp Saga

2. **Saga Implementation** (1 giờ)
   - Xem `SagaOrchestrator.java`
   - Xem saga steps: `InventoryStep`, `PaymentStep`, `NotificationStep`
   - Hiểu compensation logic

3. **Test Saga** (1 giờ)
   - Tạo order, xem saga execution
   - Test: Saga success flow
   - Test: Saga failure và compensation

#### Thực Hành

**Exercise**: Thêm một saga step mới (ví dụ: ShippingStep)

#### Câu Hỏi Tự Kiểm Tra

- [ ] Tại sao không dùng 2PC?
- [ ] Compensation là gì?
- [ ] Orchestration vs Choreography?

#### Tài Liệu Tham Khảo
- [SAGA_PATTERN_IMPLEMENTATION.md](./SAGA_PATTERN_IMPLEMENTATION.md)

---

### Tuần 7: Advanced Topics

#### Mục Tiêu
- Hiểu performance optimization
- Hiểu monitoring
- Hiểu production considerations

#### Nhiệm Vụ

1. **Performance** (1 giờ)
   - Batching trong producer
   - Concurrency trong consumer
   - Partitioning strategy

2. **Monitoring** (1 giờ)
   - Consumer lag monitoring
   - Error rate monitoring
   - Throughput monitoring

3. **Production Ready** (1 giờ)
   - Security (SSL, SASL)
   - Schema Registry
   - Multi-datacenter

#### Thực Hành

**Exercise**: Optimize producer config cho high throughput

#### Tài Liệu Tham Khảo
- [KAFKA_LEARNING_PATH.md](./KAFKA_LEARNING_PATH.md) - Level 4, 6

---

### Tuần 8: Build Your Own

#### Mục Tiêu
- Apply kiến thức đã học
- Build một feature mới
- Present và share

#### Nhiệm Vụ

1. **Design** (2 giờ)
   - Chọn một use case mới
   - Design architecture
   - Design event flow

2. **Implement** (4 giờ)
   - Implement producer
   - Implement consumers
   - Add error handling
   - Add database

3. **Test & Document** (2 giờ)
   - Test thoroughly
   - Write documentation
   - Present solution

#### Ideas

- Notification System
- Inventory Management System
- User Activity Tracking
- Real-time Analytics Dashboard

---

## 🎓 Tips Học Tập

### 1. Học Bằng Cách Làm
- Đừng chỉ đọc, hãy code
- Thử nghiệm với các config khác nhau
- Break things và fix them

### 2. Hiểu Từng Bước
- Đừng vội, hiểu từng concept một
- Đặt câu hỏi và tìm câu trả lời
- Trace code để hiểu flow

### 3. Thực Hành Thường Xuyên
- Code mỗi ngày
- Thử các scenarios khác nhau
- Build small projects

### 4. Tài Nguyên Học Tập
- Đọc Kafka official docs
- Xem video tutorials
- Join communities (Stack Overflow, Reddit)

## 📝 Checklist Tiến Độ

### Tuần 1-2: Foundation
- [ ] Setup project thành công
- [ ] Hiểu message flow
- [ ] Hiểu Topics và Partitions
- [ ] Hiểu Consumer Groups

### Tuần 3-4: Architecture
- [ ] Hiểu Event-Driven Architecture
- [ ] Implement error handling
- [ ] Hiểu retry mechanism
- [ ] Hiểu Dead Letter Queue

### Tuần 5-6: Advanced
- [ ] Implement database persistence
- [ ] Hiểu Saga Pattern
- [ ] Implement compensation
- [ ] Test distributed transactions

### Tuần 7-8: Mastery
- [ ] Optimize performance
- [ ] Implement monitoring
- [ ] Build feature mới
- [ ] Present solution

## 🎯 Kết Luận

Sau 8 tuần, bạn sẽ có:
- ✅ Kiến thức vững về Kafka
- ✅ Kinh nghiệm với Event-Driven Architecture
- ✅ Khả năng build microservices systems
- ✅ Portfolio project để showcase

**Chúc bạn học tập thành công! 🚀**
