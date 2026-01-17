# 🛒 E-Commerce Order Processing System với Kafka

> Hệ thống xử lý đơn hàng E-Commerce sử dụng Apache Kafka và Spring Boot, minh họa các pattern và best practices trong microservices architecture.

## 📚 Mục Lục

- [Tổng Quan](#-tổng-quan)
- [Kiến Trúc Hệ Thống](#-kiến-trúc-hệ-thống)
- [Tính Năng](#-tính-năng)
- [Bắt Đầu Nhanh](#-bắt-đầu-nhanh)
- [Tài Liệu Học Tập](#-tài-liệu-học-tập)
- [Cấu Trúc Project](#-cấu-trúc-project)

## 🎯 Tổng Quan

Đây là một project học tập về **Apache Kafka** và **Event-Driven Architecture** thông qua việc xây dựng một hệ thống E-Commerce Order Processing hoàn chỉnh.

### Mục Tiêu Học Tập

- ✅ Hiểu cách sử dụng Kafka trong microservices
- ✅ Thực hành Event-Driven Architecture
- ✅ Học các Kafka patterns: Consumer Groups, Partitions, Offsets
- ✅ Xử lý lỗi và Retry Mechanism
- ✅ Distributed Transactions với Saga Pattern
- ✅ Database persistence trong event-driven systems

## 🏗️ Kiến Trúc Hệ Thống

```
┌─────────────────────────────────────────────────────────────┐
│                    User (Frontend/API)                       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Order Service (Producer)                       │
│  - REST API: POST /api/v1/orders                           │
│  - Lưu order vào database                                  │
│  - Gửi event: order.created                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
            ┌───────────────────────┐
            │   Kafka Cluster       │
            │  Topic: order.created  │
            │  (3 partitions)       │
            └───────────┬────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Inventory   │ │ Notification │ │   Payment    │
│   Service    │ │   Service    │ │   Service    │
│              │ │              │ │              │
│ Group: 1    │ │ │ Group: 2    │ │ │ Group: 3    │ │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                 │                 │
       │                 │                 │
       ▼                 ▼                 ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ order.       │ │   (Log)      │ │ order.paid   │
│ cancelled    │ │              │ │ order.       │
│              │ │              │ │ cancelled    │
└──────────────┘ └──────────────┘ └──────────────┘
```

## ✨ Tính Năng

### Core Features

- ✅ **Event-Driven Architecture**: Services communicate qua Kafka events
- ✅ **Multiple Consumers**: 3 services consume từ cùng topic với consumer groups riêng
- ✅ **Database Persistence**: Lưu orders vào H2 database
- ✅ **Error Handling**: Retry mechanism với exponential backoff
- ✅ **Dead Letter Queue**: Xử lý messages bị lỗi
- ✅ **Saga Pattern**: Distributed transaction handling
- ✅ **Idempotent Producer**: Tránh duplicate messages

### Advanced Features

- ✅ **Manual Commit**: Control khi nào commit offset
- ✅ **Concurrency**: Xử lý song song nhiều messages
- ✅ **JSON Serialization**: Serialize/deserialize OrderDto
- ✅ **State Tracking**: Track saga execution state

## 🚀 Bắt Đầu Nhanh

### Yêu Cầu

- Java 21+
- Maven 3.6+
- Docker & Docker Compose

### Bước 1: Start Kafka Cluster

```bash
docker-compose up -d
```

Kiểm tra Kafka đã chạy:
```bash
docker ps
```

### Bước 2: Tạo Kafka Topics

**Windows PowerShell**:
```powershell
.\create-topics.ps1
```

**Linux/Mac**:
```bash
chmod +x create-topics.sh
./create-topics.sh
```

### Bước 3: Start Services

**Terminal 1 - Producer**:
```bash
cd producer
mvn spring-boot:run
```

**Terminal 2 - Consumer**:
```bash
cd consumer
mvn spring-boot:run
```

### Bước 4: Test API

Tạo một order:
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
      }
    ]
  }'
```

Xem logs để thấy các services xử lý order!

## 📖 Tài Liệu Học Tập

### Cho Người Mới Bắt Đầu

1. **[QUICK_START.md](./QUICK_START.md)** - Hướng dẫn setup và chạy project nhanh nhất
2. **[KAFKA_LEARNING_PATH.md](./KAFKA_LEARNING_PATH.md)** - Lộ trình học Kafka từ cơ bản đến nâng cao

### Implementation Guides

3. **[E_COMMERCE_IMPLEMENTATION.md](./E_COMMERCE_IMPLEMENTATION.md)** - Chi tiết implementation của hệ thống
4. **[DATABASE_IMPLEMENTATION.md](./DATABASE_IMPLEMENTATION.md)** - Hướng dẫn database persistence
5. **[RETRY_MECHANISM.md](./RETRY_MECHANISM.md)** - Retry mechanism với exponential backoff
6. **[SAGA_PATTERN_IMPLEMENTATION.md](./SAGA_PATTERN_IMPLEMENTATION.md)** - Saga Pattern cho distributed transactions

### Learning Resources

7. **[LEARNING_GUIDE.md](./LEARNING_GUIDE.md)** - Hướng dẫn học từng bước với examples
8. **[PROJECT_EXAMPLES.md](./PROJECT_EXAMPLES.md)** - Các ví dụ và use cases

## 📁 Cấu Trúc Project

```
playground-spring-kafka/
├── producer/                    # Order Service (Producer)
│   ├── src/main/java/
│   │   └── com/hunglp/producer/
│   │       ├── api/            # REST Controllers
│   │       ├── config/         # Kafka Configuration
│   │       ├── dto/            # Data Transfer Objects
│   │       ├── entity/         # JPA Entities
│   │       ├── mapper/         # Entity-DTO Mappers
│   │       ├── repository/     # JPA Repositories
│   │       └── service/        # Business Logic
│   └── src/main/resources/
│       └── application.yml     # Configuration
│
├── consumer/                    # Consumer Services
│   ├── src/main/java/
│   │   └── com/hunglp/consumer/
│   │       ├── api/            # REST Controllers
│   │       ├── config/         # Kafka & Retry Config
│   │       ├── dto/            # DTOs
│   │       ├── entity/         # JPA Entities
│   │       ├── handler/        # Error Handlers
│   │       ├── mapper/         # Mappers
│   │       ├── repository/     # Repositories
│   │       ├── saga/           # Saga Pattern
│   │       └── service/        # Consumer Services
│   └── src/main/resources/
│       └── application.properties
│
├── docker-compose.yml          # Kafka Cluster Setup
├── create-topics.ps1          # Create Topics Script (Windows)
├── create-topics.sh            # Create Topics Script (Linux/Mac)
└── *.md                        # Documentation Files
```

## 🎓 Learning Path

### Level 1: Basics (Tuần 1-2)
1. Đọc [QUICK_START.md](./QUICK_START.md)
2. Chạy project và test API
3. Hiểu flow: Producer → Kafka → Consumer
4. Xem logs và hiểu message flow

### Level 2: Understanding (Tuần 3-4)
1. Đọc [E_COMMERCE_IMPLEMENTATION.md](./E_COMMERCE_IMPLEMENTATION.md)
2. Hiểu Consumer Groups và Partitions
3. Thử nghiệm với multiple consumers
4. Query database để xem orders

### Level 3: Advanced (Tuần 5-6)
1. Đọc [RETRY_MECHANISM.md](./RETRY_MECHANISM.md)
2. Hiểu error handling và retry
3. Đọc [SAGA_PATTERN_IMPLEMENTATION.md](./SAGA_PATTERN_IMPLEMENTATION.md)
4. Hiểu distributed transactions

### Level 4: Mastery (Tuần 7+)
1. Tùy chỉnh và mở rộng project
2. Thêm features mới
3. Optimize performance
4. Deploy lên production environment

## 🧪 Testing

### Test Scenarios

1. **Happy Path**: Order → Inventory → Payment → Notification → Success
2. **Inventory Out of Stock**: Order → Inventory fails → Order cancelled
3. **Payment Fails**: Order → Payment fails → Retry → Success/Fail
4. **Consumer Crashes**: Order → Consumer crashes → Restart → Continue processing

### Monitor Commands

```bash
# List all topics
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker exec kafka1 kafka-console-consumer \
  --topic order.created \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Check consumer groups
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Check consumer lag
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group inventory-service-group \
  --describe
```

## 🔧 Configuration

### Producer Configuration
- **Port**: 8080
- **Database**: H2 (in-memory)
- **H2 Console**: http://localhost:8080/h2-console

### Consumer Configuration
- **Port**: 9090
- **Database**: H2 (in-memory)
- **H2 Console**: http://localhost:9090/h2-console

### Kafka Configuration
- **Brokers**: localhost:9092, localhost:9093
- **Topics**: order.created, order.paid, order.cancelled, dlq.order.failed, saga.events

## 📝 Notes

- Project này dùng cho mục đích học tập
- Services sử dụng simulation logic (không thực sự gọi payment gateway, email service)
- Database là H2 in-memory (data sẽ mất khi restart)
- Có thể mở rộng với real services và production database

## 🤝 Contributing

Đây là project học tập, bạn có thể:
- Fork và customize theo nhu cầu
- Thêm features mới
- Cải thiện documentation
- Share với cộng đồng

## 📄 License

MIT License - Free to use for learning purposes

## 🙏 Acknowledgments

Project này được tạo để học Kafka và Event-Driven Architecture patterns.

---

**Happy Learning! 🚀**

Nếu có câu hỏi, hãy đọc các file documentation hoặc xem code examples trong project.
