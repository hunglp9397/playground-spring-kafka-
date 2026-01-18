package com.hunglp.producer.config;

import com.hunglp.producer.dto.Message;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * High-Performance Producer Configuration
 * 
 * Optimizations:
 * - Batching: Giảm số lượng network requests
 * - Compression: Giảm network bandwidth
 * - Idempotence: Tránh duplicate messages
 * - Transactions: Exactly-once semantics
 * - Async sending: Non-blocking
 */
@Configuration
public class HighPerformanceProducerConfig {

    @Bean
    public ProducerFactory<String, Message> highPerformanceProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // Bootstrap servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        
        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // ========== IDEMPOTENCE & EXACTLY-ONCE ==========
        // Enable idempotence: Đảm bảo không gửi duplicate messages
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Required for idempotence
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // ========== TRANSACTIONS (Exactly-Once Semantics) ==========
        // Transactional ID: Required for exactly-once semantics
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "high-perf-producer-1");
        
        // ========== HIGH PERFORMANCE OPTIMIZATIONS ==========
        // Batching: Gửi nhiều messages trong một request
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10); // Wait 10ms để batch
        
        // Compression: Giảm network bandwidth
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // snappy, gzip, lz4, zstd
        
        // Buffer memory: Memory cho batching
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB
        
        // ========== PERFORMANCE TUNING ==========
        // Request timeout
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        
        // Delivery timeout
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        // Metadata fetch timeout
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 300000);
        
        // ========== PARTITIONING ==========
        // Custom partitioner nếu cần (optional)
        // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Message> highPerformanceKafkaTemplate() {
        KafkaTemplate<String, Message> template = new KafkaTemplate<>(highPerformanceProducerFactory());
        
        // Enable transactions for exactly-once semantics
        template.setTransactionIdPrefix("tx-");
        
        return template;
    }

    @Bean
    public ProducerFactory<String, com.hunglp.producer.dto.ReplyMessage> replyProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, com.hunglp.producer.dto.ReplyMessage> replyKafkaTemplate() {
        return new KafkaTemplate<>(replyProducerFactory());
    }
}
