package com.hunglp.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for retry mechanism with exponential backoff
 * 
 * Retry Strategy:
 * - Max attempts: 3 (initial + 2 retries)
 * - Initial delay: 1000ms (1 second)
 * - Multiplier: 2.0 (doubles each retry)
 * - Max delay: 10000ms (10 seconds)
 * 
 * Retry delays:
 * - 1st retry: 1 second
 * - 2nd retry: 2 seconds
 * - 3rd retry: 4 seconds
 * - Max: 10 seconds
 */
@Configuration
@Slf4j
public class RetryConfig {

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // Exponential backoff policy
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000); // 1 second
        backOffPolicy.setMultiplier(2.0); // Double the delay each retry
        backOffPolicy.setMaxInterval(10000); // Max 10 seconds
        retryTemplate.setBackOffPolicy(backOffPolicy);

        // Retry policy - retry on all exceptions except specific ones
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(Exception.class, true);
        // Don't retry on IllegalArgumentException (business logic errors)
        retryableExceptions.put(IllegalArgumentException.class, false);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3, retryableExceptions);
        retryTemplate.setRetryPolicy(retryPolicy);

        // Retry listener for logging
        retryTemplate.registerListener(new org.springframework.retry.RetryListener() {
            @Override
            public <T, E extends Throwable> boolean open(org.springframework.retry.RetryContext context,
                                                          org.springframework.retry.RetryCallback<T, E> callback) {
                log.debug("Retry attempt opened");
                return true;
            }

            @Override
            public <T, E extends Throwable> void onError(org.springframework.retry.RetryContext context,
                                                         org.springframework.retry.RetryCallback<T, E> callback,
                                                         Throwable throwable) {
                int attemptNumber = context.getRetryCount() + 1;
                log.warn("Retry attempt {} failed: {}", attemptNumber, throwable.getMessage());
            }

            @Override
            public <T, E extends Throwable> void close(org.springframework.retry.RetryContext context,
                                                        org.springframework.retry.RetryCallback<T, E> callback,
                                                        Throwable throwable) {
                if (throwable != null) {
                    log.error("All retry attempts exhausted. Last error: {}", throwable.getMessage());
                } else {
                    log.debug("Retry successful");
                }
            }
        });

        return retryTemplate;
    }
}
