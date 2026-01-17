# ⚡ Quick Start Guide

Hướng dẫn nhanh để chạy project E-Commerce Order Processing System.

## 📋 Yêu Cầu

- ✅ Java 21 hoặc cao hơn
- ✅ Maven 3.6+ 
- ✅ Docker & Docker Compose
- ✅ Git

## 🚀 5 Bước Để Chạy Project

### Bước 1: Clone và Di Chuyển Vào Project

```bash
cd playground-spring-kafka
```

### Bước 2: Start Kafka Cluster

```bash
docker-compose up -d
```

Đợi vài giây để Kafka khởi động. Kiểm tra:
```bash
docker ps
```

Bạn sẽ thấy 2 containers: `kafka1` và `kafka2`

### Bước 3: Tạo Kafka Topics

**Windows (PowerShell)**:
```powershell
.\create-topics.ps1
```

**Linux/Mac**:
```bash
chmod +x create-topics.sh
./create-topics.sh
```

**Kết quả mong đợi**:
```
Creating Kafka topics for E-Commerce Order Processing System...
Topics created successfully!
```

### Bước 4: Start Services

Mở **2 terminal windows**:

**Terminal 1 - Producer (Order Service)**:
```bash
cd producer
mvn spring-boot:run
```

**Terminal 2 - Consumer (All Services)**:
```bash
cd consumer
mvn spring-boot:run
```

Đợi cả 2 services khởi động hoàn tất. Bạn sẽ thấy:
- Producer: `Started ProducerApplication`
- Consumer: `Started ConsumerApplication`

### Bước 5: Test API

Tạo một order đơn giản:

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

**Response mong đợi**:
```json
{
  "orderId": "uuid-here",
  "userId": "user123",
  "items": [...],
  "totalAmount": 1000.00,
  "status": "CREATED",
  "createdAt": "2024-01-01T10:00:00"
}
```

## 👀 Xem Kết Quả

### 1. Xem Logs

Trong **Terminal 2 (Consumer)**, bạn sẽ thấy:

```
Inventory Service - Processing order: orderId=xxx
Notification Service - Sending order confirmation email: orderId=xxx
Payment Service - Processing payment: orderId=xxx
Order Status Listener - Order paid: orderId=xxx
```

### 2. Query Database

**Producer Database** (H2 Console):
- URL: http://localhost:8080/h2-console
- JDBC URL: `jdbc:h2:mem:orderdb`
- Username: `sa`
- Password: (để trống)

**Query**:
```sql
SELECT * FROM orders;
SELECT * FROM order_items;
```

**Consumer Database** (H2 Console):
- URL: http://localhost:9090/h2-console
- JDBC URL: `jdbc:h2:mem:orderdb`
- Username: `sa`
- Password: (để trống)

### 3. Monitor Kafka Topics

Xem messages trong Kafka:

```bash
# Xem order.created events
docker exec kafka1 kafka-console-consumer \
  --topic order.created \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Xem order.paid events
docker exec kafka1 kafka-console-consumer \
  --topic order.paid \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

## 🎯 Test Scenarios

### Scenario 1: Happy Path (Thành Công)

1. Tạo order như trên
2. Xem logs: Tất cả services xử lý thành công
3. Check database: Order status = "PAID"

### Scenario 2: Inventory Out of Stock

Để test inventory failure, bạn có thể:
- Tạo nhiều orders liên tiếp (10% sẽ fail inventory check)
- Xem logs: "Insufficient inventory, cancelling order"
- Check database: Order status = "CANCELLED"

### Scenario 3: Payment Failure

Để test payment failure:
- Tạo nhiều orders (15% sẽ fail payment)
- Xem logs: "Payment failed"
- Check database: Order status = "FAILED"

## 🔍 Troubleshooting

### Lỗi: Kafka không start

```bash
# Check logs
docker-compose logs kafka1

# Restart
docker-compose restart
```

### Lỗi: Topics không tạo được

```bash
# Check Kafka đã sẵn sàng
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092

# Nếu empty, đợi thêm vài giây rồi thử lại
```

### Lỗi: Port đã được sử dụng

```bash
# Check port 8080
netstat -ano | findstr :8080  # Windows
lsof -i :8080                 # Mac/Linux

# Hoặc đổi port trong application.yml
```

### Lỗi: Consumer không nhận messages

1. Check consumer đã start chưa
2. Check consumer group:
```bash
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```
3. Check consumer lag:
```bash
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group inventory-service-group \
  --describe
```

## 📚 Next Steps

Sau khi chạy thành công:

1. ✅ Đọc [E_COMMERCE_IMPLEMENTATION.md](./E_COMMERCE_IMPLEMENTATION.md) để hiểu chi tiết
2. ✅ Đọc [LEARNING_GUIDE.md](./LEARNING_GUIDE.md) để học từng bước
3. ✅ Thử nghiệm với các scenarios khác nhau
4. ✅ Xem code để hiểu implementation

## 💡 Tips

- **Luôn check logs** để hiểu flow
- **Query database** để xem data changes
- **Monitor Kafka topics** để xem messages
- **Thử nhiều scenarios** để hiểu error handling

---

**Chúc bạn học tập vui vẻ! 🎉**
