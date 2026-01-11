package com.hunglp.consumer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hunglp.consumer.dto.OrderDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Custom error handler that sends failed messages to Dead Letter Queue
 * after all retry attempts are exhausted
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class CustomErrorHandler implements CommonErrorHandler {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
                                    MessageListenerContainer container, boolean batchListener) {
        log.error("Kafka listener container error: {}", thrownException.getMessage(), thrownException);
    }

    @Override
    public void handleRecord(Exception thrownException, ConsumerRecord<?, ?> record,
                            Consumer<?, ?> consumer, MessageListenerContainer container) {
        log.error("Error processing record: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key(), thrownException);

        try {
            // Extract order information from the record
            Object value = record.value();
            String orderId = record.key() != null ? record.key().toString() : "unknown";
            
            if (value instanceof OrderDto) {
                OrderDto order = (OrderDto) value;
                sendToDLQ(orderId, order, thrownException);
            } else if (value instanceof String) {
                // Handle string messages
                String dlqMessage = String.format(
                        "Failed to process message. Key: %s, Error: %s, Message: %s",
                        orderId, thrownException.getMessage(), value
                );
                kafkaTemplate.send("dlq.order.failed", orderId, dlqMessage);
            } else {
                // Serialize unknown type to JSON
                String jsonValue = objectMapper.writeValueAsString(value);
                String dlqMessage = String.format(
                        "Failed to process message. Key: %s, Error: %s, Message: %s",
                        orderId, thrownException.getMessage(), jsonValue
                );
                kafkaTemplate.send("dlq.order.failed", orderId, dlqMessage);
            }

            log.warn("Message sent to DLQ: topic={}, key={}", record.topic(), orderId);

        } catch (Exception e) {
            log.error("Failed to send message to DLQ: topic={}, key={}",
                    record.topic(), record.key(), e);
        }
    }

    private void sendToDLQ(String orderId, OrderDto order, Throwable exception) {
        try {
            String dlqMessage = String.format(
                    "Failed to process order after all retries. OrderId: %s, Error: %s, Order: %s",
                    orderId, exception.getMessage(), objectMapper.writeValueAsString(order)
            );

            kafkaTemplate.send("dlq.order.failed", orderId, dlqMessage)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.warn("DLQ - Message sent to dead letter queue: orderId={}, error={}",
                                    orderId, exception.getMessage());
                        } else {
                            log.error("DLQ - Failed to send message to DLQ: orderId={}", orderId, ex);
                        }
                    });
        } catch (Exception e) {
            log.error("DLQ - Error creating DLQ message: orderId={}", orderId, e);
        }
    }
}
