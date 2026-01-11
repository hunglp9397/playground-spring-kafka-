package com.hunglp.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hunglp.consumer.streams.AnalyticsStreamsProcessor;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
@RequiredArgsConstructor
public class KafkaStreamsBuilderConfig {

    private final StreamsBuilder streamsBuilder;
    private final AnalyticsStreamsProcessor analyticsStreamsProcessor;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @PostConstruct
    public void buildPipeline() {
        analyticsStreamsProcessor.buildPipeline(streamsBuilder);
    }
}
