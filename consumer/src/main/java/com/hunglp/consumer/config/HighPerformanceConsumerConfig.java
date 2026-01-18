package com.hunglp.consumer.config;

import com.hunglp.consumer.dto.Message;
import com.hunglp.consumer.dto.ReplyMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * High-Performance Consumer Configuration
 * 
 * Optimizations:
 * - Concurrency: Xử lý song song nhiều partitions
 * - Batch processing: Xử lý nhiều messages cùng lúc
 * - Exactly-once: Transactional consumers
 * - Manual commit: Control khi nào commit
 * - Fetch optimization: Tăng throughput
 */
@Configuration
@EnableKafka
public class HighPerformanceConsumerConfig {

    @Bean
    public ConsumerFactory<String, Message> highPerformanceConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // Bootstrap servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        
        // Group ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "high-performance-consumer-group");
        
        // Deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Message.class);
        
        // ========== EXACTLY-ONCE SEMANTICS ==========
        // Isolation level: read_committed để chỉ đọc committed messages
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        
        // ========== MANUAL COMMIT (Đảm bảo không mất message) ==========
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // ========== HIGH PERFORMANCE OPTIMIZATIONS ==========
        // Fetch size: Lấy nhiều messages một lúc
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // 1KB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // Wait max 500ms
        
        // Max poll records: Số lượng messages mỗi lần poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // Process 500 messages at once
        
        // Session timeout
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        
        // Heartbeat interval
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        
        // ========== OFFSET MANAGEMENT ==========
        // Auto offset reset: earliest để không bỏ lỡ messages
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // ========== CONSUMER IDEMPOTENCY ==========
        // Enable idempotent consumer (check duplicate processing)
        // Implemented in service layer với messageId tracking
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Message>
    highPerformanceKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(highPerformanceConsumerFactory());
        
        // ========== MANUAL ACKNOWLEDGMENT ==========
        // Chỉ commit sau khi xử lý thành công
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        // ========== CONCURRENCY (High Performance) ==========
        // Xử lý song song nhiều partitions
        // Số threads = số partitions (hoặc ít hơn)
        factory.setConcurrency(3); // 3 threads để xử lý song song
        
        // ========== BATCH LISTENER ==========
        // Enable batch processing
        factory.setBatchListener(true);
        
        return factory;
    }

    // Consumer factory cho reply messages
    @Bean
    public ConsumerFactory<String, ReplyMessage> replyConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reply-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ReplyMessage.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ReplyMessage>
    replyKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ReplyMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(replyConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    // Producer factory cho sending replies
    @Bean
    public ProducerFactory<String, ReplyMessage> replyProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                "localhost:9092,localhost:9093");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                JsonSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        return new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ReplyMessage> replyKafkaTemplate() {
        return new KafkaTemplate<>(replyProducerFactory());
    }
}
