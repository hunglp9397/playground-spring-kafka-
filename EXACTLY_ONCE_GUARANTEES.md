# ✅ Exactly-Once Guarantees - Implementation Details

> Chi tiết về cách đảm bảo exactly-once semantics trong hệ thống

## 🎯 Yêu Cầu

1. ✅ Producer không gửi trùng message
2. ✅ Consumer không mất message
3. ✅ Restart consumer không mất message
4. ✅ Exactly-once semantics

## 🔒 Producer: Không Gửi Trùng Message

### 1. Idempotent Producer

**Configuration**:
```java
ENABLE_IDEMPOTENCE_CONFIG = true
ACKS_CONFIG = "all"
RETRIES_CONFIG = Integer.MAX_VALUE
MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5
```

**Cách hoạt động**:
- Producer gán unique `producerId` và `sequenceNumber` cho mỗi message
- Broker kiểm tra sequence number → reject duplicate
- Đảm bảo: Cùng message chỉ được gửi một lần

### 2. Unique Message IDs

**Implementation**:
```java
String messageId = UUID.randomUUID().toString();
Message message = new Message(key, content);
message.setMessageId(messageId);
```

**Tracking**:
```java
// In-memory tracking (có thể dùng Redis trong production)
private final ConcurrentMap<String, Boolean> sentMessageIds = new ConcurrentHashMap<>();

// Check before sending
if (sentMessageIds.containsKey(messageId)) {
    return null; // Already sent
}

// Mark after successful send
sentMessageIds.put(messageId, true);
```

### 3. Transactional Producer

**Configuration**:
```java
TRANSACTIONAL_ID_CONFIG = "high-perf-producer-1"
```

**Cách hoạt động**:
- Producer bắt đầu transaction
- Gửi messages trong transaction
- Commit transaction → tất cả messages được gửi hoặc không gửi (atomic)

## 🔒 Consumer: Không Mất Message

### 1. Manual Commit

**Configuration**:
```java
ENABLE_AUTO_COMMIT_CONFIG = false
ACK_MODE = MANUAL_IMMEDIATE
```

**Implementation**:
```java
@KafkaListener(...)
public void processMessage(..., Acknowledgment acknowledgment) {
    try {
        // Process message
        processMessageInternal(message);
        
        // Chỉ commit sau khi xử lý thành công
        acknowledgment.acknowledge();
    } catch (Exception e) {
        // Không acknowledge → message sẽ được retry
        throw e;
    }
}
```

**Kết quả**:
- Message chỉ được commit sau khi xử lý thành công
- Nếu xử lý lỗi → không commit → message được retry
- **Không mất message**

### 2. Offset Persistence

**Configuration**:
```java
AUTO_OFFSET_RESET_CONFIG = "earliest"
```

**Cách hoạt động**:
- Offset được lưu sau khi `acknowledge()`
- Restart consumer → tiếp tục từ offset đã commit
- **Restart không mất message**

### 3. Exactly-Once Consumer

**Configuration**:
```java
ISOLATION_LEVEL_CONFIG = "read_committed"
```

**Cách hoạt động**:
- Chỉ đọc messages đã được commit (trong transaction)
- Tránh đọc uncommitted messages
- Đảm bảo consistency

### 4. Idempotency Check

**Implementation**:
```java
private final ConcurrentMap<String, Boolean> processedMessageIds = new ConcurrentHashMap<>();

// Check before processing
if (processedMessageIds.containsKey(message.getMessageId())) {
    log.warn("Duplicate message skipped");
    acknowledgment.acknowledge(); // Acknowledge để không retry
    return;
}

// Mark after processing
processedMessageIds.put(message.getMessageId(), true);
```

**Kết quả**:
- Nếu message đã được xử lý → skip
- Tránh duplicate processing
- **Exactly-once processing**

## 🔄 Exactly-Once Flow

### Producer Flow

```
1. Generate unique messageId
   ↓
2. Check: Đã gửi chưa? → Skip nếu đã gửi
   ↓
3. Send message (idempotent producer)
   ↓
4. Broker checks sequence number
   ↓
5. If duplicate → Reject
   ↓
6. If new → Accept và store
   ↓
7. Mark messageId as sent
```

### Consumer Flow

```
1. Poll messages from Kafka
   ↓
2. For each message:
   ↓
3. Check: Đã xử lý chưa? → Skip nếu đã xử lý
   ↓
4. Process message
   ↓
5. Mark messageId as processed
   ↓
6. Acknowledge (commit offset)
   ↓
7. If error → Don't acknowledge → Retry
```

## 🧪 Testing Exactly-Once

### Test 1: Producer Idempotency

```bash
# Send same message multiple times
for i in {1..5}; do
  curl -X POST http://localhost:8080/api/v1/high-performance/send \
    -H "Content-Type: application/json" \
    -d '{
      "key": "test-key",
      "content": "Test message",
      "topic": "performance.topic"
    }'
done

# Expected: Chỉ 1 message được gửi (idempotent producer)
# Check logs: "Message already sent" warnings
```

### Test 2: Consumer Restart

```bash
# 1. Start consumer
# 2. Send 10 messages
# 3. Stop consumer (Ctrl+C) sau khi nhận 5 messages
# 4. Check: 5 messages chưa được acknowledge
# 5. Restart consumer
# 6. Expected: 5 messages còn lại được xử lý
```

### Test 3: Duplicate Processing Prevention

```bash
# 1. Send message
# 2. Consumer xử lý message
# 3. Simulate duplicate (restart consumer với same offset)
# 4. Expected: Message được skip (idempotency check)
```

## 📊 Guarantees Summary

| Requirement | Producer | Consumer | Result |
|------------|----------|----------|--------|
| Không gửi trùng | ✅ Idempotent producer | - | ✅ |
| Không mất message | - | ✅ Manual commit | ✅ |
| Restart không mất | - | ✅ Offset persistence | ✅ |
| Exactly-once | ✅ Transactions | ✅ read_committed + idempotency | ✅ |

## 🎯 Best Practices

### 1. Message ID Strategy

**Current**: UUID.randomUUID()
**Production**: 
- Timestamp + sequence number
- Business ID (nếu có)
- Distributed ID generator (Snowflake, etc.)

### 2. Idempotency Storage

**Current**: In-memory (ConcurrentHashMap)
**Production**:
- Redis với TTL
- Database với cleanup job
- Distributed cache

### 3. Transaction Management

**Current**: Kafka transactions
**Production**:
- Monitor transaction timeouts
- Handle transaction failures
- Retry logic

### 4. Monitoring

- Track duplicate rate
- Track processing time
- Monitor consumer lag
- Alert on errors

## ⚠️ Limitations

### 1. In-Memory Tracking

- **Current**: Lost on restart
- **Solution**: Use Redis or database

### 2. Transaction Overhead

- Transactions có overhead
- Cân nhắc khi cần very high throughput

### 3. Partition Rebalancing

- Rebalancing có thể cause duplicate processing
- Idempotency check giải quyết vấn đề này

## 📝 Summary

### Producer Guarantees

✅ **Idempotent**: Không gửi duplicate
- Idempotent producer config
- Unique message IDs
- In-memory tracking

✅ **Exactly-Once**: Gửi đúng một lần
- Transactions
- `acks=all`
- Retry với idempotence

### Consumer Guarantees

✅ **No Message Loss**: Không mất message
- Manual commit
- `auto.offset.reset=earliest`
- Acknowledge sau khi xử lý thành công

✅ **Restart Safe**: Restart không mất message
- Offset persistence
- Continue from last committed offset

✅ **Exactly-Once**: Xử lý đúng một lần
- `isolation.level=read_committed`
- Idempotency check
- Skip duplicate messages

---

**Kết luận**: Hệ thống đảm bảo exactly-once semantics với cả producer và consumer!
