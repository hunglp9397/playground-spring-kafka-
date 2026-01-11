package com.hunglp.consumer.service;

import com.hunglp.consumer.dto.AnalyticsMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AnalyticsService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public AnalyticsMetrics getMetricsForWindow(Duration windowDuration) {
        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || !kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
                return createEmptyMetrics();
            }

            Instant now = Instant.now();
            Instant windowStart = now.minus(windowDuration);

            AnalyticsMetrics metrics = new AnalyticsMetrics();
            metrics.setWindowStart(LocalDateTime.ofInstant(windowStart, ZoneId.systemDefault()).toString());
            metrics.setWindowEnd(LocalDateTime.ofInstant(now, ZoneId.systemDefault()).toString());

            // Query clicks per page
            ReadOnlyWindowStore<String, Long> clicksStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType("clicks-per-page-store",
                            QueryableStoreTypes.windowStore())
            );
            Map<String, Long> clicksByPage = queryWindowStore(clicksStore, windowStart, now);
            metrics.setClicksByPage(clicksByPage);
            metrics.setTotalClicks(clicksByPage.values().stream().mapToLong(Long::longValue).sum());

            // Query views per page
            ReadOnlyWindowStore<String, Long> viewsStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType("views-per-page-store",
                            QueryableStoreTypes.windowStore())
            );
            Map<String, Long> viewsByPage = queryWindowStore(viewsStore, windowStart, now);
            metrics.setViewsByPage(viewsByPage);
            metrics.setTotalViews(viewsByPage.values().stream().mapToLong(Long::longValue).sum());

            // Query revenue per product
            ReadOnlyWindowStore<String, BigDecimal> revenueStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType("revenue-per-product-store",
                            QueryableStoreTypes.windowStore())
            );
            Map<String, BigDecimal> revenueByProduct = queryRevenueStore(revenueStore, windowStart, now);
            metrics.setRevenueByProduct(revenueByProduct);
            metrics.setTotalRevenue(revenueByProduct.values().stream()
                    .reduce(BigDecimal.ZERO, BigDecimal::add));

            // Calculate conversion rate
            if (metrics.getTotalViews() > 0) {
                metrics.setConversionRate((double) metrics.getTotalPurchases() / metrics.getTotalViews());
            } else {
                metrics.setConversionRate(0.0);
            }

            return metrics;

        } catch (Exception e) {
            log.error("Error querying analytics metrics", e);
            return createEmptyMetrics();
        }
    }

    private <V> Map<String, V> queryWindowStore(ReadOnlyWindowStore<String, V> store,
                                               Instant windowStart, Instant windowEnd) {
        Map<String, V> result = new HashMap<>();
        try (KeyValueIterator<org.apache.kafka.streams.state.Windowed<String>, V> iterator =
                     store.fetchAll(windowStart, windowEnd)) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                String key = entry.key.key();
                V value = entry.value;
                result.merge(key, value, (v1, v2) -> {
                    if (v1 instanceof Long && v2 instanceof Long) {
                        return (V) Long.valueOf((Long) v1 + (Long) v2);
                    }
                    return v1;
                });
            }
        }
        return result;
    }

    private Map<String, BigDecimal> queryRevenueStore(ReadOnlyWindowStore<String, BigDecimal> store,
                                                      Instant windowStart, Instant windowEnd) {
        Map<String, BigDecimal> result = new HashMap<>();
        try (KeyValueIterator<org.apache.kafka.streams.state.Windowed<String>, BigDecimal> iterator =
                     store.fetchAll(windowStart, windowEnd)) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                String key = entry.key.key();
                BigDecimal value = entry.value;
                result.merge(key, value, BigDecimal::add);
            }
        }
        return result;
    }

    private AnalyticsMetrics createEmptyMetrics() {
        AnalyticsMetrics metrics = new AnalyticsMetrics();
        metrics.setTotalClicks(0L);
        metrics.setTotalViews(0L);
        metrics.setTotalPurchases(0L);
        metrics.setTotalRevenue(BigDecimal.ZERO);
        metrics.setUniqueUsers(0L);
        metrics.setUniqueSessions(0L);
        metrics.setClicksByPage(new HashMap<>());
        metrics.setViewsByPage(new HashMap<>());
        metrics.setRevenueByProduct(new HashMap<>());
        metrics.setConversionRate(0.0);
        metrics.setAverageSessionDuration(0.0);
        return metrics;
    }
}
