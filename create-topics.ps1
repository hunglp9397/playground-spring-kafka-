# PowerShell script to create Kafka topics for E-Commerce Order Processing System

Write-Host "Creating Kafka topics for E-Commerce Order Processing System..." -ForegroundColor Green

# Order created topic (3 partitions for load distribution)
docker exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create `
  --topic order.created `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

# Order paid topic
docker exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create `
  --topic order.paid `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

# Order cancelled topic
docker exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create `
  --topic order.cancelled `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

# Dead Letter Queue topic
docker exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create `
  --topic dlq.order.failed `
  --bootstrap-server localhost:9092 `
  --partitions 1 `
  --replication-factor 2 `
  --if-not-exists

# Saga events topic
docker exec -it kafka1  /opt/kafka/bin/kafka-topics.sh --create `
  --topic saga.events `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

Write-Host "`nTopics created successfully!" -ForegroundColor Green
Write-Host "`nListing all topics:" -ForegroundColor Yellow
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
