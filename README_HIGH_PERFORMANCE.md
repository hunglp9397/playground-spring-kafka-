# ⚡ High-Performance Kafka System

> Pet project minh họa các kỹ thuật đạt high performance và đảm bảo reliability trong Kafka

## 🎯 Yêu Cầu Đã Đáp Ứng

### ✅ Producer
- ✅ High performance (batching, compression, async)
- ✅ Không gửi trùng message (idempotent)
- ✅ Exactly-once semantics (transactions)

### ✅ Consumer
- ✅ High performance (concurrency, batch processing)
- ✅ Không mất message (manual commit)
- ✅ Restart không mất message (offset persistence)
- ✅ Exactly-once semantics (read_committed + idempotency)
- ✅ Đảm bảo thứ tự message (key-based partitioning)

### ✅ Patterns
- ✅ Request-Reply pattern
- ✅ Partition strategy cho từng use case

## 🚀 Quick Start

### 1. Start Kafka

```bash
docker-compose up -d
```

### 2. Create Topics

```powershell
.\create-high-performance-topics.ps1
```

### 3. Start Services

**Producer**:
```bash
cd producer
mvn spring-boot:run
```

**Consumer**:
```bash
cd consumer
mvn spring-boot:run
```

## 🧪 Testing

### Test High Performance

```bash
curl -X POST "http://localhost:8080/api/v1/high-performance/performance-test?topic=performance.topic&messageCount=10000&batchSize=100"
```

### Test Idempotency

```bash
curl -X POST http://localhost:8080/api/v1/high-performance/send \
  -H "Content-Type: application/json" \
  -d '{
    "key": "test-key",
    "content": "Test message",
    "topic": "performance.topic"
  }'
```

### Test Request-Reply

```bash
curl -X POST http://localhost:8080/api/v1/high-performance/request-reply \
  -H "Content-Type: application/json" \
  -d '{
    "key": "request-key",
    "content": "Request message",
    "topic": "request.topic",
    "waitForReply": true,
    "timeoutMs": 5000
  }'
```

## 📚 Documentation

- **[HIGH_PERFORMANCE_KAFKA.md](./HIGH_PERFORMANCE_KAFKA.md)** - Chi tiết implementation
- **[PARTITION_STRATEGY_GUIDE.md](./PARTITION_STRATEGY_GUIDE.md)** - Hướng dẫn chọn partitions

## 🎓 Learning Points

1. **Idempotency**: Unique message IDs + idempotent producer
2. **Exactly-Once**: Transactions + read_committed
3. **High Performance**: Batching + compression + concurrency
4. **Message Ordering**: Key-based partitioning
5. **Request-Reply**: Correlation ID matching

---

**Happy Learning! 🚀**
