package com.hunglp.consumer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Aggregated analytics metrics
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AnalyticsMetrics {
    private String windowStart;
    private String windowEnd;
    private Long totalClicks;
    private Long totalViews;
    private Long totalPurchases;
    private BigDecimal totalRevenue;
    private Long uniqueUsers;
    private Long uniqueSessions;
    private Map<String, Long> clicksByPage;
    private Map<String, Long> viewsByPage;
    private Map<String, BigDecimal> revenueByProduct;
    private Double conversionRate; // purchases / views
    private Double averageSessionDuration; // milliseconds
}
