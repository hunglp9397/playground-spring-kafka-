# 💾 Database Implementation - E-Commerce Order Processing

## 📋 Tổng Quan

Đã implement database persistence cho E-Commerce Order Processing System sử dụng:
- **JPA/Hibernate** cho ORM
- **H2 Database** (in-memory) cho development
- **Spring Data JPA** cho repository pattern

## 🗄️ Database Schema

### Tables

#### `orders`
```sql
CREATE TABLE orders (
    order_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    total_amount DECIMAL(19,2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP
);
```

#### `order_items`
```sql
CREATE TABLE order_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(19,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
```

## 🏗️ Implementation

### 1. Dependencies

Đã thêm vào `producer/pom.xml` và `consumer/pom.xml`:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>
```

### 2. Entity Classes

#### Producer Entities

**Location**: `producer/src/main/java/com/hunglp/producer/entity/`

- **Order.java**: Order entity với JPA annotations
- **OrderItem.java**: OrderItem entity với relationship to Order

**Key Features**:
- `@PrePersist` và `@PreUpdate` hooks để tự động set timestamps
- `@OneToMany` relationship với cascade operations
- `@Entity` và `@Table` annotations

#### Consumer Entities

**Location**: `consumer/src/main/java/com/hunglp/consumer/entity/`

- **Order.java**: Tương tự producer entity
- **OrderItem.java**: Tương tự producer entity

### 3. Repository Interfaces

#### Producer Repository

**Location**: `producer/src/main/java/com/hunglp/producer/repository/OrderRepository.java`

```java
@Repository
public interface OrderRepository extends JpaRepository<Order, String> {
    Optional<Order> findByOrderId(String orderId);
    List<Order> findByUserId(String userId);
    List<Order> findByStatus(String status);
}
```

#### Consumer Repository

**Location**: `consumer/src/main/java/com/hunglp/consumer/repository/OrderRepository.java`

Tương tự producer repository.

### 4. Mapper Classes

**OrderMapper**: Convert giữa Entity và DTO

**Location**: 
- `producer/src/main/java/com/hunglp/producer/mapper/OrderMapper.java`
- `consumer/src/main/java/com/hunglp/consumer/mapper/OrderMapper.java`

### 5. Service Updates

#### Producer - OrderService

**Changes**:
- Lưu order vào database trước khi gửi event
- Thêm methods: `getOrder()`, `getOrdersByUser()`

```java
@Transactional
public OrderDto createOrder(CreateOrderRequest request) {
    // ... create order DTO
    
    // Save to database
    Order orderEntity = orderMapper.toEntity(orderDto);
    orderEntity = orderRepository.save(orderEntity);
    
    // Send to Kafka
    orderKafkaTemplate.send("order.created", orderId, orderDto);
    
    return orderDto;
}
```

#### Consumer Services

**InventoryService**:
- `saveOrUpdateOrder()`: Lưu/update order khi nhận event
- `updateOrderStatus()`: Update order status

**PaymentService**:
- `saveOrUpdateOrder()`: Lưu/update order khi xử lý payment
- `updateOrderStatus()`: Update status thành PAID hoặc FAILED

**OrderStatusListener**:
- `updateOrderStatus()`: Update status khi nhận order.paid hoặc order.cancelled events

### 6. Configuration

#### Producer - application.yml

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:orderdb
    driver-class-name: org.h2.Driver
    username: sa
    password:
  h2:
    console:
      enabled: true
      path: /h2-console
  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop
    show-sql: true
```

#### Consumer - application.properties

```properties
spring.datasource.url=jdbc:h2:mem:orderdb
spring.datasource.driver-class-name=org.h2.Driver
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true
```

## 🔄 Data Flow

### Order Creation Flow

```
1. User creates order via REST API
   ↓
2. OrderService.createOrder()
   ↓
3. Save order to database (producer DB)
   ↓
4. Send order.created event to Kafka
   ↓
5. Consumer services receive event
   ↓
6. Each service saves/updates order in database (consumer DB)
   ↓
7. Update order status based on processing result
```

### Status Updates

```
Order Created → Database (status: CREATED)
     ↓
Inventory Check → Update status if cancelled
     ↓
Payment Processing → Update status (PAID/FAILED)
     ↓
Order Status Events → Update status in database
```

## 🧪 Testing

### 1. Access H2 Console

**Producer**:
```
http://localhost:8080/h2-console
JDBC URL: jdbc:h2:mem:orderdb
Username: sa
Password: (empty)
```

**Consumer**:
```
http://localhost:9090/h2-console
JDBC URL: jdbc:h2:mem:orderdb
Username: sa
Password: (empty)
```

### 2. Query Orders

```sql
-- Get all orders
SELECT * FROM orders;

-- Get order by ID
SELECT * FROM orders WHERE order_id = 'xxx';

-- Get orders by user
SELECT * FROM orders WHERE user_id = 'user123';

-- Get orders by status
SELECT * FROM orders WHERE status = 'PAID';

-- Get order with items
SELECT o.*, oi.* 
FROM orders o 
LEFT JOIN order_items oi ON o.order_id = oi.order_id 
WHERE o.order_id = 'xxx';
```

### 3. Test API Endpoints

**Create Order**:
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

**Get Order**:
```bash
curl http://localhost:8080/api/v1/orders/{orderId}
```

**Get Orders by User**:
```bash
curl http://localhost:8080/api/v1/orders/user/user123
```

## 📊 Key Features

### ✅ 1. Transaction Management

- `@Transactional` trên service methods
- Đảm bảo data consistency
- Rollback nếu có lỗi

### ✅ 2. Entity Relationships

- `@OneToMany` relationship giữa Order và OrderItem
- Cascade operations (CASCADE.ALL)
- Orphan removal

### ✅ 3. Automatic Timestamps

- `@PrePersist`: Set createdAt khi tạo mới
- `@PreUpdate`: Set updatedAt khi update

### ✅ 4. Repository Pattern

- Spring Data JPA repositories
- Custom query methods
- Type-safe queries

### ✅ 5. DTO Mapping

- Separate Entity và DTO
- Mapper classes để convert
- Clean separation of concerns

## 🔍 Best Practices

### 1. Separate Databases

- Producer và Consumer có database riêng
- Trong production, có thể dùng shared database hoặc separate databases

### 2. Idempotency

- Check order exists trước khi tạo mới
- Update thay vì create duplicate

### 3. Status Tracking

- Track order status changes
- Timestamp mỗi status change
- Audit trail

### 4. Error Handling

- Transaction rollback on errors
- Retry mechanism với database operations
- Dead letter queue cho failed updates

## 🚀 Production Considerations

### 1. Database Choice

- **Development**: H2 (in-memory)
- **Production**: PostgreSQL, MySQL, hoặc Oracle
- Connection pooling
- Read replicas cho scaling

### 2. Migration

- Use Flyway hoặc Liquibase
- Version control cho schema changes
- Rollback strategy

### 3. Performance

- Indexes trên frequently queried columns
- Pagination cho large result sets
- Caching cho frequently accessed data

### 4. Monitoring

- Database connection pool metrics
- Query performance monitoring
- Slow query logging

## 📝 Notes

- H2 database là in-memory, data sẽ mất khi restart application
- Trong production, sử dụng persistent database
- Consider using shared database hoặc event sourcing pattern
- Database schema được tự động tạo bởi Hibernate (`ddl-auto: create-drop`)
