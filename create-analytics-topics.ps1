# PowerShell script to create Kafka topics for Real-time Analytics System

Write-Host "Creating Kafka topics for Real-time Analytics System..." -ForegroundColor Green

# Analytics event topics
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create `
  --topic analytics.user.click `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create `
  --topic analytics.user.view `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create `
  --topic analytics.user.purchase `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 2 `
  --if-not-exists

Write-Host "`nTopics created successfully!" -ForegroundColor Green
Write-Host "`nListing analytics topics:" -ForegroundColor Yellow
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092 | Select-String "analytics"
