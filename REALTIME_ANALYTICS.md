# 📊 Real-time Analytics System - Implementation Guide

## 📋 Tổng Quan

Hệ thống Real-time Analytics sử dụng Kafka và Kafka Streams để xử lý và aggregate analytics events theo thời gian thực.

## 🏗️ Architecture

```
User Actions → Analytics Producer → Kafka Topics
                                    ↓
                            Kafka Streams Processor
                                    ↓
                            Aggregated Metrics
                                    ↓
                            Analytics API
```

## 🚀 Components

### Producer (Analytics Event Producer)

**Location**: `producer/src/main/java/com/hunglp/producer/`

#### Event Types

1. **UserClickEvent**: User click events
   - `pageUrl`, `elementId`, `elementType`, `referrer`

2. **UserViewEvent**: Page view events
   - `pageUrl`, `pageTitle`, `duration`, `referrer`

3. **UserPurchaseEvent**: Purchase events
   - `orderId`, `items`, `totalAmount`, `paymentMethod`, `currency`

#### API Endpoints

- `POST /api/v1/analytics/events/click` - Track click event
- `POST /api/v1/analytics/events/view` - Track view event
- `POST /api/v1/analytics/events/purchase` - Track purchase event
- `POST /api/v1/analytics/demo/generate?count=10` - Generate demo events

### Consumer (Analytics Streams Processor)

**Location**: `consumer/src/main/java/com/hunglp/consumer/`

#### Kafka Streams Processing

1. **Click Stream Processing**:
   - Count clicks per page (1-minute windows)
   - Output: `analytics.metrics.clicks-per-page`

2. **View Stream Processing**:
   - Count views per page (1-minute windows)
   - Calculate average session duration
   - Output: `analytics.metrics.views-per-page`, `analytics.metrics.avg-session-duration`

3. **Purchase Stream Processing**:
   - Calculate revenue per product (1-minute windows)
   - Count total purchases
   - Output: `analytics.metrics.revenue-per-product`, `analytics.metrics.total-purchases`

4. **Combined Metrics**:
   - Count unique users
   - Output: `analytics.metrics.unique-users`

#### Analytics API

- `GET /api/v1/analytics/metrics?minutes=60` - Get metrics for last N minutes
- `GET /api/v1/analytics/metrics/realtime` - Get real-time metrics (last 1 minute)
- `GET /api/v1/analytics/metrics/hourly` - Get hourly metrics
- `GET /api/v1/analytics/health` - Health check

## 🔧 Setup & Configuration

### 1. Start Kafka Cluster

```bash
docker-compose up -d
```

### 2. Create Topics

```bash
# Analytics event topics
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic analytics.user.click \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2 \
  --if-not-exists

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic analytics.user.view \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2 \
  --if-not-exists

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic analytics.user.purchase \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2 \
  --if-not-exists

# Metrics output topics (auto-created by Kafka Streams)
```

### 3. Start Services

**Producer**:
```bash
cd producer
mvn spring-boot:run
# Runs on port 8080
```

**Consumer**:
```bash
cd consumer
mvn spring-boot:run
# Runs on port 9090
```

## 🧪 Testing

### 1. Generate Demo Events

```bash
curl -X POST "http://localhost:8080/api/v1/analytics/demo/generate?count=100"
```

### 2. Track Custom Events

**Click Event**:
```bash
curl -X POST http://localhost:8080/api/v1/analytics/events/click \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "sessionId": "session456",
    "pageUrl": "/products/laptop",
    "elementId": "btn-add-to-cart",
    "elementType": "button",
    "referrer": "/products"
  }'
```

**View Event**:
```bash
curl -X POST http://localhost:8080/api/v1/analytics/events/view \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "sessionId": "session456",
    "pageUrl": "/products/laptop",
    "pageTitle": "Laptop Product Page",
    "duration": 5000,
    "referrer": "/products"
  }'
```

**Purchase Event**:
```bash
curl -X POST http://localhost:8080/api/v1/analytics/events/purchase \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "sessionId": "session456",
    "orderId": "order789",
    "items": [
      {
        "productId": "prod1",
        "productName": "Laptop",
        "quantity": 1,
        "price": 1000.00
      }
    ],
    "totalAmount": 1000.00,
    "paymentMethod": "CREDIT_CARD",
    "currency": "USD"
  }'
```

### 3. Query Analytics Metrics

**Real-time Metrics**:
```bash
curl http://localhost:9090/api/v1/analytics/metrics/realtime
```

**Last 60 Minutes**:
```bash
curl http://localhost:9090/api/v1/analytics/metrics?minutes=60
```

**Hourly Metrics**:
```bash
curl http://localhost:9090/api/v1/analytics/metrics/hourly
```

## 📊 Metrics Response Example

```json
{
  "windowStart": "2024-01-01T10:00:00",
  "windowEnd": "2024-01-01T11:00:00",
  "totalClicks": 150,
  "totalViews": 200,
  "totalPurchases": 25,
  "totalRevenue": 25000.00,
  "uniqueUsers": 50,
  "uniqueSessions": 75,
  "clicksByPage": {
    "/products/laptop": 50,
    "/products/phone": 100
  },
  "viewsByPage": {
    "/products/laptop": 80,
    "/products/phone": 120
  },
  "revenueByProduct": {
    "prod1": 10000.00,
    "prod2": 15000.00
  },
  "conversionRate": 0.125,
  "averageSessionDuration": 4500.0
}
```

## 🎯 Key Features

### ✅ 1. Real-time Processing

- Kafka Streams xử lý events theo thời gian thực
- Time-windowed aggregations (1-minute windows)
- Low latency processing

### ✅ 2. Multiple Aggregations

- Clicks per page
- Views per page
- Revenue per product
- Unique users
- Conversion rate
- Average session duration

### ✅ 3. Scalable Architecture

- Kafka Streams tự động scale
- State stores cho efficient querying
- Exactly-once processing guarantees

### ✅ 4. RESTful API

- Query metrics by time window
- Real-time metrics endpoint
- Health check endpoint

## 🔍 Kafka Streams Concepts

### Time Windows

- **Tumbling Windows**: Fixed-size, non-overlapping windows (1 minute)
- **Hopping Windows**: Fixed-size, overlapping windows
- **Session Windows**: Dynamic windows based on activity

### State Stores

- **In-memory stores**: Fast but limited size
- **RocksDB stores**: Persistent, larger capacity
- **Queryable stores**: Can query from external applications

### Processing Guarantees

- **At-least-once**: May process events multiple times
- **Exactly-once**: Process each event exactly once (v2)

## 🚀 Production Considerations

### 1. Window Size

- Smaller windows = more real-time but more overhead
- Larger windows = less overhead but less real-time
- Consider: 1 minute for real-time, 5 minutes for hourly

### 2. State Store Management

- Monitor state store size
- Configure cleanup policies
- Consider using changelog topics for backup

### 3. Scaling

- Kafka Streams automatically scales with partitions
- More partitions = more parallelism
- Consider partition count based on throughput

### 4. Monitoring

- Monitor lag
- Monitor processing time
- Monitor state store size
- Alert on errors

## 📝 Notes

- Kafka Streams requires state stores to be queryable
- State stores are local to each instance
- For distributed queries, consider using Kafka Streams Interactive Queries
- Windowed aggregations are stored with window timestamps
