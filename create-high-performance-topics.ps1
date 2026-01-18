# PowerShell script to create Kafka topics for High-Performance System

Write-Host "Creating Kafka topics for High-Performance System..." -ForegroundColor Green

# Performance topic - High throughput, nhiều partitions
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create `
  --topic performance.topic `
  --bootstrap-server localhost:9092 `
  --partitions 6 `
  --replication-factor 2 `
  --if-not-exists

# Ordered topic - Cần ordering, ít partitions hơn
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create `
  --topic ordered.topic `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

# Request topic - Request-Reply pattern
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create `
  --topic request.topic `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

# Reply topic - Request-Reply pattern
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create `
  --topic reply.topic `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

Write-Host "`nTopics created successfully!" -ForegroundColor Green
Write-Host "`nListing topics:" -ForegroundColor Yellow
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092 | Select-String "performance|ordered|request|reply"

Write-Host "`nTopic details:" -ForegroundColor Yellow
docker exec kafka1 kafka-topics --describe --topic performance.topic --bootstrap-server localhost:9092
docker exec kafka1 kafka-topics --describe --topic ordered.topic --bootstrap-server localhost:9092
