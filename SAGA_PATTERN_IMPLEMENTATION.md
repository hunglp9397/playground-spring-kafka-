# 🔄 Saga Pattern Implementation - Distributed Transaction Handling

## 📋 Tổng Quan

Đã implement **Orchestration-based Saga Pattern** để xử lý distributed transactions trong E-Commerce Order Processing System. Saga Pattern đảm bảo data consistency across multiple microservices mà không cần distributed transactions (2PC).

## 🎯 Saga Pattern Overview

### Vấn Đề

Trong microservices architecture, không thể sử dụng traditional ACID transactions across services. Cần một pattern để:
- Đảm bảo data consistency
- Handle failures gracefully
- Rollback changes khi có lỗi

### Giải Pháp: Saga Pattern

**Saga Pattern** chia một long-running transaction thành một chuỗi các local transactions. Mỗi local transaction có một compensating transaction để rollback.

## 🏗️ Implementation

### 1. Saga Components

#### SagaState Enum

**Location**: `consumer/src/main/java/com/hunglp/consumer/saga/SagaState.java`

Các states trong saga workflow:
- `ORDER_CREATED`: Order đã được tạo
- `INVENTORY_RESERVED`: Inventory đã được reserve
- `PAYMENT_PROCESSED`: Payment đã được xử lý
- `NOTIFICATION_SENT`: Notification đã được gửi
- `COMPLETED`: Saga hoàn thành thành công
- `COMPENSATING`: Đang compensate (rollback)
- `COMPENSATED`: Đã compensate xong
- `FAILED`: Saga failed permanently

#### SagaStep Interface

**Location**: `consumer/src/main/java/com/hunglp/consumer/saga/SagaStep.java`

Interface cho các saga steps:
```java
public interface SagaStep {
    boolean execute(OrderDto order);      // Execute step
    boolean compensate(OrderDto order);   // Compensate (rollback)
    SagaState getSuccessState();          // State after success
    String getStepName();                 // Step name
}
```

### 2. Saga Steps

#### InventoryStep

**Location**: `consumer/src/main/java/com/hunglp/consumer/saga/steps/InventoryStep.java`

- **Execute**: Check và reserve inventory
- **Compensate**: Release reserved inventory
- **Success State**: `INVENTORY_RESERVED`

#### PaymentStep

**Location**: `consumer/src/main/java/com/hunglp/consumer/saga/steps/PaymentStep.java`

- **Execute**: Process payment
- **Compensate**: Refund payment
- **Success State**: `PAYMENT_PROCESSED`

#### NotificationStep

**Location**: `consumer/src/main/java/com/hunglp/consumer/saga/steps/NotificationStep.java`

- **Execute**: Send notification
- **Compensate**: Send cancellation notification (non-compensatable)
- **Success State**: `NOTIFICATION_SENT`

### 3. Saga Orchestrator

**Location**: `consumer/src/main/java/com/hunglp/consumer/service/SagaOrchestrator.java`

**Chức năng**:
- Điều phối các saga steps
- Track saga state
- Handle compensation khi có lỗi
- Lưu saga execution vào database

**Workflow**:

```
1. Receive order.created event
   ↓
2. Create SagaExecution record
   ↓
3. Execute steps sequentially:
   - InventoryStep
   - PaymentStep
   - NotificationStep
   ↓
4. If any step fails:
   - Stop execution
   - Compensate executed steps (reverse order)
   - Update saga state
   - Send failure event
   ↓
5. If all steps succeed:
   - Update saga state to COMPLETED
   - Send completion event
```

### 4. Saga Execution Tracking

#### SagaExecution Entity

**Location**: `consumer/src/main/java/com/hunglp/consumer/entity/SagaExecution.java`

Lưu trữ saga state trong database:
- `sagaId`: Unique saga identifier
- `orderId`: Associated order ID
- `currentState`: Current saga state
- `completedSteps`: List of completed steps
- `failedStep`: Step that failed (if any)
- `errorMessage`: Error message (if any)

#### SagaExecutionRepository

**Location**: `consumer/src/main/java/com/hunglp/consumer/repository/SagaExecutionRepository.java`

Repository để query saga executions.

### 5. Saga Events

#### SagaEvent DTO

**Location**: `consumer/src/main/java/com/hunglp/consumer/saga/SagaEvent.java`

Event được gửi vào Kafka topic `saga.events` để track saga progress.

## 🔄 Saga Flow

### Success Flow

```
Order Created
    ↓
Saga Started (ORDER_CREATED)
    ↓
Inventory Step → INVENTORY_RESERVED
    ↓
Payment Step → PAYMENT_PROCESSED
    ↓
Notification Step → NOTIFICATION_SENT
    ↓
Saga Completed (COMPLETED)
```

### Failure Flow (with Compensation)

```
Order Created
    ↓
Saga Started (ORDER_CREATED)
    ↓
Inventory Step → INVENTORY_RESERVED ✓
    ↓
Payment Step → FAILED ✗
    ↓
Compensation Started (COMPENSATING)
    ↓
Compensate Payment Step (already failed, skip)
    ↓
Compensate Inventory Step (release inventory) ✓
    ↓
Saga Compensated (COMPENSATED)
    ↓
Order Cancelled
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
      }
    ]
  }'
```

### 2. Monitor Saga Execution

**Check Database**:
```sql
SELECT * FROM saga_executions;
SELECT * FROM saga_executions WHERE order_id = 'xxx';
SELECT * FROM saga_executions WHERE current_state = 'COMPENSATED';
```

**Check Logs**:
```bash
# Saga started
grep "Saga Orchestrator - Starting saga" logs/consumer.log

# Step execution
grep "Saga Orchestrator - Executing step" logs/consumer.log

# Compensation
grep "Saga Orchestrator - Starting compensation" logs/consumer.log

# Saga completion
grep "Saga Orchestrator - Saga completed" logs/consumer.log
```

### 3. Monitor Saga Events

```bash
docker exec kafka1 kafka-console-consumer \
  --topic saga.events \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

## 📊 Key Features

### ✅ 1. Orchestration-Based Saga

- Central orchestrator điều phối các steps
- Easier to understand và debug
- Centralized error handling

### ✅ 2. Compensation (Rollback)

- Mỗi step có compensating transaction
- Compensate trong reverse order (LIFO)
- Đảm bảo data consistency

### ✅ 3. State Tracking

- Track saga state trong database
- Track completed steps
- Track failed steps và error messages

### ✅ 4. Event-Driven

- Saga events được gửi vào Kafka
- Có thể monitor và audit
- Support for event sourcing

### ✅ 5. Idempotency

- Steps có thể retry safely
- Compensations có thể retry safely
- Đảm bảo không duplicate operations

## 🎯 Best Practices

### 1. Compensating Transactions

- **Must be idempotent**: Có thể retry nhiều lần
- **Must be reversible**: Có thể undo original operation
- **Should be fast**: Compensate nhanh để không block

### 2. Step Design

- **Keep steps small**: Mỗi step làm một việc
- **Make steps independent**: Không phụ thuộc vào order (nếu có thể)
- **Handle failures gracefully**: Return false thay vì throw exception

### 3. State Management

- **Persist state**: Lưu state vào database
- **Track progress**: Track completed steps
- **Handle retries**: Support retry mechanism

### 4. Error Handling

- **Log errors**: Log chi tiết để debug
- **Send events**: Gửi events để notify
- **Update state**: Update saga state appropriately

## 🔍 Saga vs 2PC (Two-Phase Commit)

| Feature | Saga Pattern | 2PC |
|---------|-------------|-----|
| **Performance** | Fast (no blocking) | Slow (blocking) |
| **Scalability** | High | Low |
| **Complexity** | Medium | Low |
| **Failure Handling** | Compensation | Rollback |
| **Use Case** | Long-running transactions | Short transactions |

## 🚀 Production Considerations

### 1. Saga Timeout

- Set timeout cho saga execution
- Handle timeout appropriately
- Compensate nếu timeout

### 2. Saga Retry

- Retry failed steps
- Retry compensation nếu fail
- Exponential backoff

### 3. Monitoring

- Monitor saga execution time
- Monitor compensation rate
- Alert on saga failures

### 4. Saga Versioning

- Version saga steps
- Support backward compatibility
- Migrate old sagas

## 📝 Notes

- Saga Pattern phù hợp cho long-running transactions
- Compensation không phải lúc nào cũng perfect (e.g., notifications)
- Cần design compensating transactions carefully
- Consider using Saga framework (e.g., Temporal, Eventuate) cho production

## 🔄 Alternative: Choreography-Based Saga

Thay vì orchestration, có thể dùng choreography:
- Mỗi service tự quyết định bước tiếp theo
- Services communicate qua events
- No central orchestrator

**Pros**: More decoupled, scalable
**Cons**: Harder to understand, debug, và maintain
