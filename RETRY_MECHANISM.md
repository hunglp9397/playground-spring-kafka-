# 🔄 Retry Mechanism với Exponential Backoff - Implementation Guide

## 📋 Tổng Quan

Đã implement retry mechanism với exponential backoff cho các consumer services trong E-Commerce Order Processing System. Retry mechanism giúp xử lý các transient errors (lỗi tạm thời) như network timeout, database connection issues, etc.

## 🎯 Retry Strategy

### Configuration

**Retry Policy**:
- **Max Attempts**: 3 (initial attempt + 2 retries)
- **Initial Delay**: 1000ms (1 second)
- **Multiplier**: 2.0 (delay tăng gấp đôi mỗi lần retry)
- **Max Delay**: 10000ms (10 seconds)
- **Max Elapsed Time**: 30000ms (30 seconds)

**Retry Delays**:
```
Attempt 1: Immediate (initial attempt)
Attempt 2: Wait 1 second
Attempt 3: Wait 2 seconds
Attempt 4: Wait 4 seconds (max 10 seconds)
```

### Flow Diagram

```
Message Received
      ↓
Process Message
      ↓
   Success? ──Yes──> Acknowledge & Commit
      ↓ No
   Retry? ──Yes──> Wait (exponential backoff) ──> Retry
      ↓ No
All Retries Exhausted
      ↓
Send to Dead Letter Queue
      ↓
Commit Offset (recovered)
```

## 🏗️ Implementation

### 1. Dependencies

Đã thêm vào `consumer/pom.xml`:
```xml
<dependency>
    <groupId>org.springframework.retry</groupId>
    <artifactId>spring-retry</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework</groupId>
    <artifactId>spring-aspects</artifactId>
</dependency>
```

### 2. RetryConfig

**Location**: `consumer/src/main/java/com/hunglp/consumer/config/RetryConfig.java`

Tạo `RetryTemplate` với exponential backoff policy:

```java
@Bean
public RetryTemplate retryTemplate() {
    RetryTemplate retryTemplate = new RetryTemplate();
    
    // Exponential backoff: 1s → 2s → 4s → max 10s
    ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(1000);
    backOffPolicy.setMultiplier(2.0);
    backOffPolicy.setMaxInterval(10000);
    retryTemplate.setBackOffPolicy(backOffPolicy);
    
    // Retry policy: max 3 attempts
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3);
    retryTemplate.setRetryPolicy(retryPolicy);
    
    return retryTemplate;
}
```

### 3. CustomErrorHandler

**Location**: `consumer/src/main/java/com/hunglp/consumer/handler/CustomErrorHandler.java`

Xử lý messages sau khi hết retry, gửi vào Dead Letter Queue:

```java
@Component
public class CustomErrorHandler implements CommonErrorHandler {
    @Override
    public void handleRecord(Exception exception, ConsumerRecord<?, ?> record, ...) {
        // Send failed message to DLQ
        kafkaTemplate.send("dlq.order.failed", orderId, dlqMessage);
    }
}
```

### 4. KafkaConsumerConfig

**Location**: `consumer/src/main/java/com/hunglp/consumer/config/KafkaConsumerConfig.java`

Cấu hình error handler với exponential backoff:

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, OrderDto>
orderKafkaListenerContainerFactory(CustomErrorHandler customErrorHandler) {
    // ...
    
    ExponentialBackOff backOff = new ExponentialBackOff();
    backOff.setInitialInterval(1000);
    backOff.setMultiplier(2.0);
    backOff.setMaxInterval(10000);
    backOff.setMaxElapsedTime(30000);
    
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        (record, exception) -> {
            // Recovery: send to DLQ
            customErrorHandler.handleRecord(exception, record, null, null);
        },
        backOff
    );
    
    factory.setCommonErrorHandler(errorHandler);
    return factory;
}
```

### 5. Service Implementation

Các services sử dụng `RetryTemplate` để wrap business logic:

**InventoryService**:
```java
retryTemplate.execute(context -> {
    boolean inventoryAvailable = checkInventoryWithRetry(order);
    // ... process order
    return null;
});
```

**PaymentService**:
```java
retryTemplate.execute(context -> {
    boolean paymentSuccessful = processPaymentWithRetry(order);
    // ... process payment
    return null;
});
```

**NotificationService**:
```java
retryTemplate.execute(context -> {
    sendEmailWithRetry(order);
    return null;
});
```

## 🔄 Two Approaches

### Approach 1: RetryTemplate (Programmatic)

**Pros**:
- Full control over retry logic
- Can access retry context
- Flexible error handling

**Cons**:
- More verbose
- Need to inject RetryTemplate

**Usage**:
```java
@RequiredArgsConstructor
public class InventoryService {
    private final RetryTemplate retryTemplate;
    
    public void processOrder(...) {
        retryTemplate.execute(context -> {
            // Business logic
            return null;
        });
    }
}
```

### Approach 2: @Retryable Annotation

**Pros**:
- Cleaner code
- Declarative approach
- Less boilerplate

**Cons**:
- Less control
- Requires @EnableRetry

**Usage**:
```java
@Retryable(
    value = {Exception.class},
    maxAttempts = 3,
    backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000)
)
public void processOrderWithRetry(OrderDto order) throws Exception {
    // Business logic
}

@Recover
public void recover(Exception ex, OrderDto order) {
    // Recovery logic
}
```

**Example Service**: `RetryableOrderProcessor.java`

## 📊 Retry Behavior

### Transient Errors (Retryable)

Các lỗi sau sẽ được retry:
- `RuntimeException` (network timeout, connection issues)
- `SQLException` (database connection timeout)
- `IOException` (network issues)

### Non-Retryable Errors

Các lỗi sau sẽ KHÔNG được retry:
- `IllegalArgumentException` (business logic errors)
- `NullPointerException` (code bugs)

### Retry Logs

```
WARN  - Retry attempt 1 failed: Database connection timeout
WARN  - Retry attempt 2 failed: Database connection timeout
ERROR - All retry attempts exhausted. Last error: Database connection timeout
WARN  - DLQ - Message sent to dead letter queue: orderId=xxx
```

## 🧪 Testing Retry Mechanism

### 1. Simulate Transient Errors

Các services đã được cấu hình để simulate transient errors (20% chance):

```java
private boolean checkInventoryWithRetry(OrderDto order) throws Exception {
    // 20% chance of throwing exception
    if (ThreadLocalRandom.current().nextDouble() < 0.2) {
        throw new RuntimeException("Transient error: Database connection timeout");
    }
    // ... rest of logic
}
```

### 2. Test Scenario

1. **Create Order**:
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "items": [{"productId": "prod1", "productName": "Laptop", "quantity": 1, "price": 1000.00}]
  }'
```

2. **Observe Logs**:
   - Nếu có transient error, sẽ thấy retry attempts
   - Sau 3 attempts, message sẽ được gửi vào DLQ

3. **Check DLQ**:
```bash
docker exec kafka1 kafka-console-consumer \
  --topic dlq.order.failed \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

## 📈 Monitoring

### Metrics to Monitor

1. **Retry Count**: Số lần retry per service
2. **Retry Rate**: Tỷ lệ messages cần retry
3. **DLQ Size**: Số messages trong DLQ
4. **Average Retry Delay**: Thời gian delay trung bình

### Logs to Watch

```bash
# Retry attempts
grep "Retry attempt" logs/consumer.log

# DLQ messages
grep "DLQ - Message sent" logs/consumer.log

# All retries exhausted
grep "All retry attempts exhausted" logs/consumer.log
```

## 🎯 Best Practices

### 1. Retryable vs Non-Retryable Errors

- **Retry**: Transient errors (network, timeout, temporary unavailability)
- **Don't Retry**: Business logic errors, validation errors, permanent failures

### 2. Retry Limits

- **Max Attempts**: 3-5 attempts (không quá nhiều)
- **Max Delay**: 10-30 seconds (tránh block consumer quá lâu)
- **Total Time**: 30-60 seconds (tránh message quá cũ)

### 3. Idempotency

- Đảm bảo business logic là idempotent
- Có thể retry nhiều lần mà không gây side effects

### 4. Dead Letter Queue

- Luôn có DLQ cho messages sau khi hết retry
- Monitor DLQ để investigate issues
- Có process để retry DLQ messages manually

## 🔍 Troubleshooting

### Issue: Messages stuck in retry loop

**Solution**: Check max attempts và max elapsed time

### Issue: Too many messages in DLQ

**Solution**: 
- Investigate root cause
- Fix underlying issue
- Consider increasing retry attempts if appropriate

### Issue: Retry delays too long

**Solution**: Adjust multiplier và max delay

## 📚 References

- [Spring Retry Documentation](https://docs.spring.io/spring-retry/docs/current/reference/html/)
- [Spring Kafka Error Handling](https://docs.spring.io/spring-kafka/reference/html/#error-handling)
- [Exponential Backoff Strategy](https://en.wikipedia.org/wiki/Exponential_backoff)
