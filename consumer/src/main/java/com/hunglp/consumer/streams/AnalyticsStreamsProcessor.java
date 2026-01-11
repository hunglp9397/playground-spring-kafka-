package com.hunglp.consumer.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hunglp.consumer.dto.AnalyticsMetrics;
import com.hunglp.consumer.dto.UserClickEvent;
import com.hunglp.consumer.dto.UserPurchaseEvent;
import com.hunglp.consumer.dto.UserViewEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Streams processor for real-time analytics aggregation
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class AnalyticsStreamsProcessor {

    private final ObjectMapper objectMapper;

    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // Source streams
        KStream<String, String> clickStream = streamsBuilder.stream("analytics.user.click");
        KStream<String, String> viewStream = streamsBuilder.stream("analytics.user.view");
        KStream<String, String> purchaseStream = streamsBuilder.stream("analytics.user.purchase");

        // Process click events
        processClickStream(clickStream, streamsBuilder);
        
        // Process view events
        processViewStream(viewStream, streamsBuilder);
        
        // Process purchase events
        processPurchaseStream(purchaseStream, streamsBuilder);
        
        // Combined metrics stream (1-minute windows)
        processCombinedMetrics(clickStream, viewStream, purchaseStream, streamsBuilder);
    }

    private void processClickStream(KStream<String, String> clickStream, StreamsBuilder streamsBuilder) {
        // Count clicks per page (1-minute windows)
        clickStream
                .mapValues(this::parseClickEvent)
                .filter((key, value) -> value != null)
                .groupBy((key, event) -> event.getPageUrl())
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count(Materialized.as("clicks-per-page-store"))
                .toStream()
                .map((windowedKey, count) -> {
                    String key = windowedKey.key() + "@" + windowedKey.window().start();
                    return KeyValue.pair(key, count.toString());
                })
                .to("analytics.metrics.clicks-per-page");

        log.info("Click stream processing configured");
    }

    private void processViewStream(KStream<String, String> viewStream, StreamsBuilder streamsBuilder) {
        // Count views per page (1-minute windows)
        viewStream
                .mapValues(this::parseViewEvent)
                .filter((key, value) -> value != null)
                .groupBy((key, event) -> event.getPageUrl())
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count(Materialized.as("views-per-page-store"))
                .toStream()
                .map((windowedKey, count) -> {
                    String key = windowedKey.key() + "@" + windowedKey.window().start();
                    return KeyValue.pair(key, count.toString());
                })
                .to("analytics.metrics.views-per-page");

        // Calculate average session duration
        viewStream
                .mapValues(this::parseViewEvent)
                .filter((key, value) -> value != null && value.getDuration() != null)
                .groupBy((key, event) -> event.getSessionId())
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        () -> new SessionDurationAggregator(),
                        (key, event, aggregator) -> {
                            aggregator.addDuration(event.getDuration());
                            return aggregator;
                        },
                        Materialized.as("session-duration-store")
                )
                .toStream()
                .mapValues(aggregator -> String.valueOf(aggregator.getAverage()))
                .to("analytics.metrics.avg-session-duration");

        log.info("View stream processing configured");
    }

    private void processPurchaseStream(KStream<String, String> purchaseStream, StreamsBuilder streamsBuilder) {
        // Calculate revenue per product (1-minute windows)
        purchaseStream
                .mapValues(this::parsePurchaseEvent)
                .filter((key, value) -> value != null && value.getItems() != null)
                .flatMap((key, event) -> {
                    Map<String, BigDecimal> revenueByProduct = new HashMap<>();
                    event.getItems().forEach(item -> {
                        BigDecimal revenue = item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity()));
                        revenueByProduct.put(item.getProductId(), revenue);
                    });
                    return revenueByProduct.entrySet().stream()
                            .map(entry -> KeyValue.pair(entry.getKey(), entry.getValue().toString()))
                            .toList();
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        () -> BigDecimal.ZERO,
                        (key, value, aggregate) -> aggregate.add(new BigDecimal(value)),
                        Materialized.as("revenue-per-product-store")
                )
                .toStream()
                .map((windowedKey, revenue) -> {
                    String key = windowedKey.key() + "@" + windowedKey.window().start();
                    return KeyValue.pair(key, revenue.toString());
                })
                .to("analytics.metrics.revenue-per-product");

        // Count total purchases (1-minute windows)
        purchaseStream
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count(Materialized.as("purchases-count-store"))
                .toStream()
                .map((windowedKey, count) -> {
                    String key = "total@" + windowedKey.window().start();
                    return KeyValue.pair(key, count.toString());
                })
                .to("analytics.metrics.total-purchases");

        log.info("Purchase stream processing configured");
    }

    private void processCombinedMetrics(KStream<String, String> clickStream,
                                      KStream<String, String> viewStream,
                                      KStream<String, String> purchaseStream,
                                      StreamsBuilder streamsBuilder) {
        // Combine all streams for overall metrics
        KStream<String, String> allEvents = clickStream
                .merge(viewStream)
                .merge(purchaseStream);

        // Count unique users (1-minute windows) - simplified approach
        allEvents
                .mapValues(this::parseEventUserId)
                .filter((key, value) -> value != null)
                .groupBy((key, userId) -> "unique-users")
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count(Materialized.as("unique-users-store"))
                .toStream()
                .to("analytics.metrics.unique-users");

        log.info("Combined metrics processing configured");
    }

    private UserClickEvent parseClickEvent(String json) {
        try {
            return objectMapper.readValue(json, UserClickEvent.class);
        } catch (Exception e) {
            log.error("Failed to parse click event: {}", json, e);
            return null;
        }
    }

    private UserViewEvent parseViewEvent(String json) {
        try {
            return objectMapper.readValue(json, UserViewEvent.class);
        } catch (Exception e) {
            log.error("Failed to parse view event: {}", json, e);
            return null;
        }
    }

    private UserPurchaseEvent parsePurchaseEvent(String json) {
        try {
            return objectMapper.readValue(json, UserPurchaseEvent.class);
        } catch (Exception e) {
            log.error("Failed to parse purchase event: {}", json, e);
            return null;
        }
    }

    private String parseEventUserId(String json) {
        try {
            Map<String, Object> event = objectMapper.readValue(json, Map.class);
            return (String) event.get("userId");
        } catch (Exception e) {
            log.error("Failed to parse event userId: {}", json, e);
            return null;
        }
    }

    // Helper class for session duration aggregation
    private static class SessionDurationAggregator {
        private long totalDuration = 0;
        private long count = 0;

        public void addDuration(long duration) {
            totalDuration += duration;
            count++;
        }

        public double getAverage() {
            return count > 0 ? (double) totalDuration / count : 0.0;
        }
    }
}
