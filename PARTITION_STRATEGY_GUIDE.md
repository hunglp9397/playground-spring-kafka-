# 📊 Partition Strategy Guide - Cách Chọn Số Lượng Partitions

> Hướng dẫn chi tiết về cách chọn số lượng partitions cho từng use case

## 🎯 Tổng Quan

Số lượng partitions ảnh hưởng trực tiếp đến:
- **Throughput**: Nhiều partitions = nhiều parallelism
- **Ordering**: Ít partitions = dễ đảm bảo ordering
- **Scalability**: Partitions = max consumers có thể scale
- **Resource Usage**: Nhiều partitions = nhiều overhead

## 📐 Công Thức Cơ Bản

```
Số Partitions = max(
    Số consumers tối đa,
    Expected throughput / Single partition throughput,
    Retention size / Max partition size
)
```

### Single Partition Throughput

**Producer**:
- Small messages (< 1KB): ~50,000 msg/s
- Medium messages (1-10KB): ~20,000 msg/s
- Large messages (> 10KB): ~5,000 msg/s

**Consumer**:
- Fast processing (< 10ms): ~20,000 msg/s
- Medium processing (10-100ms): ~5,000 msg/s
- Slow processing (> 100ms): ~1,000 msg/s

## 🎯 Use Cases và Partition Strategy

### Use Case 1: High Throughput, No Ordering

**Ví dụ**: Log aggregation, metrics collection, event streaming

**Requirements**:
- High throughput (> 50K msg/s)
- Không cần ordering
- Messages độc lập

**Partition Strategy**:
```
Partitions = 12-24
Key Strategy: null key hoặc random key
```

**Lý do**:
- Nhiều partitions → nhiều parallelism
- Load được phân bổ đều
- Có thể scale consumers

**Example**:
```java
// performance.topic - 6 partitions (trong project này)
// Có thể tăng lên 12-24 cho production
```

### Use Case 2: Ordered Messages per Key

**Ví dụ**: User activity tracking, order processing, session events

**Requirements**:
- Cần ordering cho messages cùng key
- Moderate throughput
- Messages có relationship

**Partition Strategy**:
```
Partitions = 3-6
Key Strategy: Business key (userId, orderId, sessionId)
```

**Lý do**:
- Messages cùng key → cùng partition → cùng consumer
- Đảm bảo ordering per key
- Balance giữa parallelism và ordering

**Example**:
```java
// ordered.topic - 3 partitions
// Key: userId → messages của cùng user được xử lý theo thứ tự
```

### Use Case 3: Request-Reply Pattern

**Ví dụ**: RPC calls, synchronous operations

**Requirements**:
- Match request với reply
- Moderate throughput
- Low latency

**Partition Strategy**:
```
Partitions = 3-6
Key Strategy: correlationId
```

**Lý do**:
- Request và reply cùng key → cùng partition
- Dễ match request với reply
- Đảm bảo ordering cho request-reply pairs

**Example**:
```java
// request.topic, reply.topic - 3 partitions
// Key: correlationId → request và reply trong cùng partition
```

### Use Case 4: Time-Series Data

**Ví dụ**: IoT sensor data, financial ticks, metrics

**Requirements**:
- High throughput
- Time-based queries
- Retention policies

**Partition Strategy**:
```
Partitions = 12-24
Key Strategy: Time-based key (timestamp + deviceId)
```

**Lý do**:
- Nhiều partitions cho high throughput
- Time-based key → dễ query theo time range
- Có thể partition theo time windows

### Use Case 5: Low Throughput, Strict Ordering

**Ví dụ**: Configuration updates, critical state changes

**Requirements**:
- Strict global ordering
- Low throughput (< 1K msg/s)
- High reliability

**Partition Strategy**:
```
Partitions = 1
Key Strategy: Sequential key hoặc null key
```

**Lý do**:
- 1 partition → đảm bảo global ordering
- Đơn giản, dễ quản lý
- Phù hợp với low throughput

## 📊 Decision Matrix

| Use Case | Throughput | Ordering | Partitions | Key Strategy |
|----------|-----------|----------|------------|--------------|
| High Throughput | > 50K/s | Not needed | 12-24 | null/random |
| Ordered per Key | 5-20K/s | Per key | 3-6 | Business key |
| Request-Reply | 1-10K/s | Per correlation | 3-6 | correlationId |
| Time-Series | > 50K/s | Time-based | 12-24 | timestamp+id |
| Strict Ordering | < 1K/s | Global | 1 | sequential/null |

## 🔢 Tính Toán Cụ Thể

### Example 1: E-Commerce Order Processing

**Requirements**:
- 10,000 orders/second
- Ordering per orderId
- 3 consumer instances

**Calculation**:
```
Expected throughput: 10,000 orders/s
Single partition throughput: ~5,000 orders/s (với processing time 10ms)
Partitions needed: 10,000 / 5,000 = 2 partitions
Max consumers: 3
Final: max(2, 3) = 3 partitions
```

**Key Strategy**: `orderId` → messages của cùng order vào cùng partition

### Example 2: Log Aggregation

**Requirements**:
- 100,000 logs/second
- No ordering needed
- 10 consumer instances

**Calculation**:
```
Expected throughput: 100,000 logs/s
Single partition throughput: ~20,000 logs/s
Partitions needed: 100,000 / 20,000 = 5 partitions
Max consumers: 10
Final: max(5, 10) = 10 partitions
```

**Key Strategy**: `null` hoặc random → load balancing

### Example 3: User Activity Tracking

**Requirements**:
- 50,000 events/second
- Ordering per userId
- 5 consumer instances

**Calculation**:
```
Expected throughput: 50,000 events/s
Single partition throughput: ~10,000 events/s
Partitions needed: 50,000 / 10,000 = 5 partitions
Max consumers: 5
Final: max(5, 5) = 5 partitions
```

**Key Strategy**: `userId` → messages của cùng user vào cùng partition

## ⚠️ Constraints và Limitations

### Maximum Partitions

**Per Broker**:
- Recommended: < 4,000 partitions per broker
- Maximum: ~10,000 partitions per broker
- Overhead: Metadata, file handles, memory

**Per Topic**:
- Recommended: < 100 partitions per topic
- Maximum: ~1,000 partitions per topic
- Consider: Replication factor × partitions

### Minimum Partitions

**For Ordering**:
- Minimum: 1 partition (global ordering)
- Recommended: 3+ partitions (per-key ordering)

**For High Availability**:
- Minimum: replication factor (usually 3)
- Partitions: ≥ replication factor

## 🔄 Rebalancing Considerations

### Adding Partitions

**When**:
- Throughput tăng
- Cần scale consumers
- Load imbalance

**Impact**:
- ✅ Tăng parallelism
- ❌ Có thể break ordering (nếu không cẩn thận)
- ❌ Consumer rebalancing

### Reducing Partitions

**When**:
- Over-provisioned
- Reduce overhead
- Simplify management

**Impact**:
- ⚠️ **Không thể giảm partitions** (Kafka limitation)
- Phải tạo topic mới và migrate

## 💡 Best Practices

### 1. Start Conservative

- Bắt đầu với ít partitions (3-6)
- Monitor throughput và lag
- Tăng dần nếu cần

### 2. Match Consumer Concurrency

```
Consumer threads ≤ Partitions
```

**Example**:
- 6 partitions → max 6 consumer threads
- 3 partitions → max 3 consumer threads

### 3. Consider Replication

```
Total partitions = Partitions × Replication factor
```

**Example**:
- 6 partitions × 3 replication = 18 total partitions
- Cần đủ brokers để handle

### 4. Monitor và Adjust

- Monitor consumer lag
- Monitor partition size
- Adjust based on metrics

## 📈 Project Implementation

### Performance Topic (6 Partitions)

```yaml
Topic: performance.topic
Partitions: 6
Replication: 2
Use Case: High throughput, no ordering
Key Strategy: null hoặc random
Throughput: > 50K msg/s
```

**Lý do 6 partitions**:
- High throughput requirements
- Có thể scale đến 6 consumers
- Load distribution tốt
- Balance giữa performance và overhead

### Ordered Topic (3 Partitions)

```yaml
Topic: ordered.topic
Partitions: 3
Replication: 2
Use Case: Ordered messages per key
Key Strategy: Business key (userId, orderId)
Throughput: 5-20K msg/s
```

**Lý do 3 partitions**:
- Đảm bảo ordering per key
- Match với consumer concurrency (3 threads)
- Moderate throughput
- Đơn giản và dễ quản lý

### Request-Reply Topics (3 Partitions)

```yaml
Topics: request.topic, reply.topic
Partitions: 3
Replication: 2
Use Case: Request-Reply pattern
Key Strategy: correlationId
Throughput: 1-10K msg/s
```

**Lý do 3 partitions**:
- Request và reply cùng partition (cùng correlationId key)
- Moderate load (request-reply thường không quá high)
- Dễ match request với reply

## 🎓 Learning Exercises

### Exercise 1: Calculate Partitions

**Scenario**: 
- Expected throughput: 30,000 msg/s
- Processing time: 20ms per message
- Max consumers: 8

**Calculate**: Số partitions cần thiết?

**Answer**:
```
Single partition throughput: 1000ms / 20ms = 50 msg/s
Partitions needed: 30,000 / 50 = 600 partitions
Max consumers: 8
Final: max(600, 8) = 600 partitions (quá nhiều!)

Reality check: 600 partitions quá nhiều
→ Cần optimize processing time hoặc tăng consumers
→ Hoặc accept lower throughput per partition
```

### Exercise 2: Choose Strategy

**Scenario**: User session events
- 20,000 events/second
- Cần ordering per sessionId
- 4 consumer instances

**Choose**: Partition strategy?

**Answer**:
```
Partitions: 4-6 (match với consumers, đảm bảo ordering)
Key: sessionId
Strategy: Key-based partitioning
```

## 📝 Summary

### Quick Reference

| Throughput | Ordering | Partitions | Key |
|-----------|----------|------------|-----|
| High (>50K/s) | No | 12-24 | null/random |
| Medium (10-50K/s) | Per key | 3-6 | Business key |
| Low (<10K/s) | Global | 1 | sequential |
| Low (<10K/s) | Per key | 3 | Business key |

### Golden Rules

1. **Partitions ≥ Consumers**: Để scale consumers
2. **Partitions ≤ 100**: Tránh overhead
3. **Key for Ordering**: Dùng business key nếu cần ordering
4. **Monitor và Adjust**: Không set và quên
5. **Start Small**: Bắt đầu với ít partitions, tăng dần

---

**Remember**: Không có công thức chung cho mọi use case. Phải analyze requirements và test!
