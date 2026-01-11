package com.hunglp.producer.api;

import com.hunglp.producer.dto.MessageDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/produce")
@Slf4j
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaTemplate kafkaTemplate;


    @PostMapping("with-key")
    public void sendWithKey(@RequestBody MessageDto messageDto){
        kafkaTemplate.send("topic.demo.single", messageDto.getMessage());

    }

    @PostMapping("without-key")
    public void sendWithoutKey(@RequestBody MessageDto messageDto) throws InterruptedException {
        for (int i = 1; i <= 1000000; i++) {
            kafkaTemplate.send("topic.test-order", "New_Messsage" + i);
            Thread.sleep(100);
        }


    }

    @PostMapping("multi-partition/with-key")
    public void sendMutiPartitionWithKey(@RequestBody MessageDto messageDto){
        for(int i = 0;i  < 10; i ++){
            kafkaTemplate.send("topic.demo.multi.3.partition", messageDto.getKey(), messageDto.getMessage() + "_" + i);
        }

    }

    @PostMapping("multi-partition/without-key")
    public void sendMutiWithoutKey(@RequestBody MessageDto messageDto) {
        for(int i = 0;i  < 100000; i ++){
            kafkaTemplate.send("topic.demo.multi.3.partition", messageDto.getMessage() + "_" + i);
        }
    }
}
