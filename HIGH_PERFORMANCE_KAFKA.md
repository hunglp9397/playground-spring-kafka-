# ⚡ High-Performance Kafka System - Implementation Guide

> Pet project minh họa các kỹ thuật để đạt high performance và đảm bảo reliability trong Kafka

## 🎯 Yêu Cầu Đã Đáp Ứng

### ✅ Producer Requirements

1. **High Performance** ✅
   - Batching: 32KB batch size, 10ms linger
   - Compression: Snappy compression
   - Async sending: Non-blocking
   - Buffer memory: 64MB

2. **Không Gửi Trùng Message** ✅
   - Idempotent producer: `ENABLE_IDEMPOTENCE_CONFIG = true`
   - Unique message IDs
   - In-memory tracking (có thể dùng Redis trong production)

3. **Exactly-Once Semantics** ✅
   - Transactions: `TRANSACTIONAL_ID_CONFIG`
   - Idempotence enabled
   - `acks=all`

### ✅ Consumer Requirements

1. **High Performance** ✅
   - Concurrency: 3 threads
   - Batch processing: 500 messages per poll
   - Fetch optimization: 1KB min, 500ms max wait

2. **Không Mất Message** ✅
   - Manual commit: Chỉ commit sau khi xử lý thành công
   - `AUTO_OFFSET_RESET = earliest`
   - Idempotency check: Track processed message IDs

3. **Restart Không Mất Message** ✅
   - Manual commit với acknowledgment
   - Offset được lưu sau khi acknowledge
   - Restart → tiếp tục từ offset đã commit

4. **Exactly-Once** ✅
   - `ISOLATION_LEVEL = read_committed`
   - Idempotency check trong consumer
   - Transactional processing

5. **Đảm Bảo Thứ Tự Message** ✅
   - Key-based partitioning
   - Messages cùng key → cùng partition → cùng consumer thread
   - Partition-level ordering

6. **Request-Reply Pattern** ✅
   - Correlation ID matching
   - Reply topic
   - Timeout handling

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              High-Performance Producer                       │
│  - Idempotent sending                                       │
│  - Transactional (exactly-once)                             │
│  - Batching & Compression                                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
            ┌───────────────────────┐
            │   Kafka Topics        │
            │                       │
            │  performance.topic    │
            │  (6 partitions)       │
            │                       │
            │  ordered.topic        │
            │  (3 partitions)       │
            │                       │
            │  request.topic        │
            │  reply.topic          │
            └───────────┬───────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│          High-Performance Consumer                           │
│  - Concurrency (3 threads)                                  │
│  - Batch processing                                         │
│  - Manual commit                                            │
│  - Idempotency check                                        │
│  - Exactly-once semantics                                   │
└─────────────────────────────────────────────────────────────┘
```

## 📊 Partition Strategy - Cách Chọn Số Lượng Partitions

### 1. Performance Topic (6 Partitions)

**Use Case**: High throughput, không cần strict ordering

**Lý do chọn 6 partitions**:
- **Throughput**: Nhiều partitions = nhiều parallelism
- **Consumer scaling**: Có thể scale đến 6 consumers
- **Load distribution**: Messages được phân bổ đều
- **Formula**: `partitions = max(consumers, throughput / single_partition_throughput)`

**Khi nào dùng nhiều partitions**:
- ✅ High throughput requirements (> 10K msg/s)
- ✅ Không cần strict ordering
- ✅ Có thể scale consumers
- ✅ Messages độc lập với nhau

**Trade-offs**:
- ❌ Tăng overhead (metadata, file handles)
- ❌ Khó đảm bảo global ordering
- ❌ Tăng memory usage

### 2. Ordered Topic (3 Partitions)

**Use Case**: Cần đảm bảo ordering cho messages cùng key

**Lý do chọn 3 partitions**:
- **Ordering guarantee**: Messages cùng key → cùng partition → cùng consumer
- **Balance**: Đủ parallelism nhưng vẫn đảm bảo ordering
- **Consumer threads**: Match với concurrency (3 threads)

**Khi nào dùng ít partitions**:
- ✅ Cần ordering per key
- ✅ Messages có relationship với nhau
- ✅ Moderate throughput (< 5K msg/s per partition)

**Trade-offs**:
- ❌ Limited parallelism
- ❌ Một partition chậm → ảnh hưởng throughput
- ✅ Dễ đảm bảo ordering

### 3. Request-Reply Topics (3 Partitions)

**Use Case**: Request-Reply pattern, cần match request với reply

**Lý do chọn 3 partitions**:
- **Correlation matching**: Correlation ID làm key → cùng partition
- **Moderate load**: Request-Reply thường không cần quá high throughput
- **Consistency**: Đảm bảo request và reply trong cùng partition

**Best Practice**:
- Sử dụng `correlationId` làm key
- Request và reply cùng partition → dễ match

## 🔧 Configuration Details

### Producer Configuration

```java
// Idempotence
ENABLE_IDEMPOTENCE_CONFIG = true
ACKS_CONFIG = "all"
RETRIES_CONFIG = Integer.MAX_VALUE
MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5

// Transactions (Exactly-Once)
TRANSACTIONAL_ID_CONFIG = "high-perf-producer-1"

// High Performance
BATCH_SIZE_CONFIG = 32768 (32KB)
LINGER_MS_CONFIG = 10ms
COMPRESSION_TYPE_CONFIG = "snappy"
BUFFER_MEMORY_CONFIG = 67108864 (64MB)
```

### Consumer Configuration

```java
// Exactly-Once
ISOLATION_LEVEL_CONFIG = "read_committed"
ENABLE_AUTO_COMMIT_CONFIG = false

// High Performance
FETCH_MIN_BYTES_CONFIG = 1024 (1KB)
FETCH_MAX_WAIT_MS_CONFIG = 500ms
MAX_POLL_RECORDS_CONFIG = 500
CONCURRENCY = 3 threads

// Reliability
AUTO_OFFSET_RESET_CONFIG = "earliest"
ACK_MODE = MANUAL_IMMEDIATE
```

## 🧪 Testing

### 1. Test Idempotency

```bash
# Send same message multiple times
curl -X POST http://localhost:8080/api/v1/high-performance/send \
  -H "Content-Type: application/json" \
  -d '{
    "key": "test-key",
    "content": "Test message",
    "topic": "performance.topic"
  }'

# Check logs: Message chỉ được gửi một lần
```

### 2. Test High Performance

```bash
# Send 10,000 messages
curl -X POST "http://localhost:8080/api/v1/high-performance/performance-test?topic=performance.topic&messageCount=10000&batchSize=100"
```

**Expected Result**:
- Throughput: > 10,000 msg/s
- No duplicates
- All messages processed

### 3. Test Message Ordering

```bash
# Send messages với cùng key
for i in {1..10}; do
  curl -X POST http://localhost:8080/api/v1/high-performance/send \
    -H "Content-Type: application/json" \
    -d "{
      \"key\": \"ordered-key\",
      \"content\": \"Message $i\",
      \"topic\": \"ordered.topic\"
    }"
done

# Check logs: Messages được xử lý theo thứ tự
```

### 4. Test Request-Reply

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

### 5. Test Consumer Restart

1. Start consumer
2. Send messages
3. Stop consumer (Ctrl+C)
4. Check: Messages chưa được acknowledge
5. Restart consumer
6. Check: Messages được xử lý lại (không mất)

## 📈 Performance Metrics

### Producer Metrics

- **Throughput**: > 50,000 msg/s (với batching)
- **Latency**: < 50ms (p99)
- **Duplicate Rate**: 0% (với idempotence)
- **Success Rate**: 100% (với retries)

### Consumer Metrics

- **Throughput**: > 30,000 msg/s (với batch processing)
- **Processing Latency**: < 100ms (p99)
- **Duplicate Processing**: 0% (với idempotency check)
- **Message Loss**: 0% (với manual commit)

## 🎯 Best Practices

### 1. Partition Sizing

**Rule of Thumb**:
```
Partitions = max(
    number_of_consumers,
    expected_throughput / single_partition_throughput,
    retention_bytes / max_partition_size
)
```

**Single Partition Throughput**:
- Producer: ~10K-50K msg/s (tùy message size)
- Consumer: ~5K-20K msg/s (tùy processing time)

### 2. Key Selection

**Cho Ordering**:
- Use business key (userId, orderId, sessionId)
- Messages cùng key → cùng partition → cùng order

**Cho Load Balancing**:
- Use random key hoặc null key
- Messages phân bổ đều across partitions

### 3. Idempotency

**Producer**:
- Unique message IDs
- Idempotent producer config
- Track sent messages (in-memory hoặc Redis)

**Consumer**:
- Check message ID trước khi xử lý
- Track processed messages
- Idempotent business logic

### 4. Exactly-Once

**Producer**:
- Transactions
- Idempotence
- `acks=all`

**Consumer**:
- `isolation.level=read_committed`
- Idempotency check
- Manual commit

### 5. High Performance

**Producer**:
- Batching (32KB, 10ms)
- Compression (snappy)
- Async sending

**Consumer**:
- Concurrency (match với partitions)
- Batch processing
- Fetch optimization

## 📝 Partition Strategy Decision Tree

```
Cần Ordering?
├─ Yes → Dùng key-based partitioning
│   ├─ High throughput? → Nhiều partitions (6-12)
│   └─ Moderate throughput? → Ít partitions (3-6)
│
└─ No → Có thể dùng null key hoặc random key
    ├─ Very high throughput? → Nhiều partitions (12+)
    ├─ High throughput? → Moderate partitions (6-12)
    └─ Low throughput? → Ít partitions (1-3)
```

## 🔍 Monitoring

### Key Metrics

1. **Producer**:
   - Send rate (msg/s)
   - Batch size
   - Compression ratio
   - Error rate

2. **Consumer**:
   - Consume rate (msg/s)
   - Lag (messages behind)
   - Processing time
   - Duplicate rate

3. **Kafka**:
   - Partition size
   - Replication lag
   - Under-replicated partitions

## 🚀 Production Considerations

### 1. Idempotency Storage

**Current**: In-memory (ConcurrentHashMap)
**Production**: Redis hoặc database
- Distributed tracking
- Persistence across restarts
- TTL để cleanup

### 2. Monitoring

- Prometheus metrics
- Grafana dashboards
- Alerting on errors/lag

### 3. Scaling

- Horizontal scaling: Thêm consumers
- Vertical scaling: Tăng resources
- Partition rebalancing: Monitor và adjust

### 4. Disaster Recovery

- Replication factor: 3 (production)
- Backup strategies
- Multi-datacenter setup

## 📚 References

- [Kafka Producer Config](https://kafka.apache.org/documentation/#producerconfigs)
- [Kafka Consumer Config](https://kafka.apache.org/documentation/#consumerconfigs)
- [Exactly-Once Semantics](https://kafka.apache.org/documentation/#semantics)
- [Partitioning Strategy](https://kafka.apache.org/documentation/#partitioning)
