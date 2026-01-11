package com.hunglp.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {

    @KafkaListener(
            topics = "topic.test-order",
            groupId = "test-order-group"
    )
    public void listen(String message,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("partition={} offset={} received message={}", partition, offset, message);
    }


    @KafkaListener(
            topics = "topic.demo.multi.3.partition", // 3 partition
            groupId = "demo-group"
    )
    public void listenMultiPartition(String message,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                     @Header(KafkaHeaders.OFFSET) long offset){

        log.info("partition={} offset={} received message={}", partition, offset, message);
    }
}
