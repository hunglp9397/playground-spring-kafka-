#!/bin/bash

# Script to create Kafka topics for E-Commerce Order Processing System

echo "Creating Kafka topics for E-Commerce Order Processing System..."

# Order created topic (3 partitions for load distribution)
docker exec kafka1 kafka-topics --create \
  --topic order.created \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2 \
  --if-not-exists

# Order paid topic
docker exec kafka1 kafka-topics --create \
  --topic order.paid \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2 \
  --if-not-exists

# Order cancelled topic
docker exec kafka1 kafka-topics --create \
  --topic order.cancelled \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2 \
  --if-not-exists

# Dead Letter Queue topic
docker exec kafka1 kafka-topics --create \
  --topic dlq.order.failed \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 2 \
  --if-not-exists

echo "Topics created successfully!"
echo ""
echo "Listing all topics:"
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
