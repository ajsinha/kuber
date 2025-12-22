/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.metrics;

import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.MeterBinder;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Prometheus Metrics Service for Kuber Cache.
 * 
 * Exposes cache metrics in Prometheus format via Spring Actuator.
 * Metrics include cache operations, memory usage, region statistics,
 * and performance counters.
 * 
 * @version 1.7.7
 */
@Slf4j
@Service
public class PrometheusMetricsService implements MeterBinder {

    @Autowired
    private CacheService cacheService;

    @Autowired
    private KuberProperties kuberProperties;

    @Autowired
    private MeterRegistry meterRegistry;

    // Operation counters
    private final AtomicLong totalGets = new AtomicLong(0);
    private final AtomicLong totalSets = new AtomicLong(0);
    private final AtomicLong totalDeletes = new AtomicLong(0);
    private final AtomicLong totalHits = new AtomicLong(0);
    private final AtomicLong totalMisses = new AtomicLong(0);
    private final AtomicLong totalEvictions = new AtomicLong(0);
    private final AtomicLong totalExpirations = new AtomicLong(0);

    // Per-region gauges
    private final Map<String, AtomicLong> regionKeyCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> regionMemoryUsage = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> regionHitRates = new ConcurrentHashMap<>();

    // Performance metrics
    private final AtomicLong avgGetLatencyMicros = new AtomicLong(0);
    private final AtomicLong avgSetLatencyMicros = new AtomicLong(0);
    private final AtomicLong requestsPerSecond = new AtomicLong(0);

    // Memory metrics
    private final AtomicLong valueCacheSize = new AtomicLong(0);
    private final AtomicLong valueCacheLimit = new AtomicLong(0);
    private final AtomicLong heapUsedBytes = new AtomicLong(0);
    private final AtomicLong heapMaxBytes = new AtomicLong(0);

    private boolean initialized = false;

    @Override
    public void bindTo(MeterRegistry registry) {
        // This is called by Spring to register meters
        log.info("Binding Kuber metrics to Prometheus registry");
    }

    @PostConstruct
    public void init() {
        if (!kuberProperties.getPrometheus().isEnabled()) {
            log.info("Prometheus metrics disabled");
            return;
        }

        log.info("Initializing Prometheus metrics for Kuber Cache");
        registerMetrics();
        initialized = true;
    }

    private void registerMetrics() {
        // Cache operation counters
        Gauge.builder("kuber_cache_gets_total", totalGets, AtomicLong::get)
                .description("Total number of GET operations")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_sets_total", totalSets, AtomicLong::get)
                .description("Total number of SET operations")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_deletes_total", totalDeletes, AtomicLong::get)
                .description("Total number of DELETE operations")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_hits_total", totalHits, AtomicLong::get)
                .description("Total number of cache hits")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_misses_total", totalMisses, AtomicLong::get)
                .description("Total number of cache misses")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_evictions_total", totalEvictions, AtomicLong::get)
                .description("Total number of evictions")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_expirations_total", totalExpirations, AtomicLong::get)
                .description("Total number of expirations")
                .tag("application", "kuber")
                .register(meterRegistry);

        // Hit rate
        Gauge.builder("kuber_cache_hit_rate", this, svc -> calculateHitRate())
                .description("Cache hit rate (0.0 to 1.0)")
                .tag("application", "kuber")
                .register(meterRegistry);

        // Performance metrics
        Gauge.builder("kuber_cache_get_latency_microseconds", avgGetLatencyMicros, AtomicLong::get)
                .description("Average GET operation latency in microseconds")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_set_latency_microseconds", avgSetLatencyMicros, AtomicLong::get)
                .description("Average SET operation latency in microseconds")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_cache_requests_per_second", requestsPerSecond, AtomicLong::get)
                .description("Requests processed per second")
                .tag("application", "kuber")
                .register(meterRegistry);

        // Memory metrics
        Gauge.builder("kuber_value_cache_size", valueCacheSize, AtomicLong::get)
                .description("Number of entries in value cache")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_value_cache_limit", valueCacheLimit, AtomicLong::get)
                .description("Maximum entries allowed in value cache")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_heap_used_bytes", heapUsedBytes, AtomicLong::get)
                .description("JVM heap memory used in bytes")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_heap_max_bytes", heapMaxBytes, AtomicLong::get)
                .description("JVM heap memory max in bytes")
                .tag("application", "kuber")
                .register(meterRegistry);

        Gauge.builder("kuber_heap_usage_ratio", this, svc -> calculateHeapUsageRatio())
                .description("JVM heap memory usage ratio (0.0 to 1.0)")
                .tag("application", "kuber")
                .register(meterRegistry);

        // Total keys across all regions
        Gauge.builder("kuber_total_keys", this, svc -> getTotalKeyCount())
                .description("Total number of keys across all regions")
                .tag("application", "kuber")
                .register(meterRegistry);

        // Total regions
        Gauge.builder("kuber_regions_count", this, svc -> getRegionCount())
                .description("Number of active cache regions")
                .tag("application", "kuber")
                .register(meterRegistry);

        // Server info
        Gauge.builder("kuber_server_info", () -> 1)
                .description("Kuber server information")
                .tag("application", "kuber")
                .tag("version", "1.7.7")
                .tag("persistence", kuberProperties.getPersistence().getType())
                .register(meterRegistry);

        log.info("Prometheus metrics registered successfully");
    }

    /**
     * Update metrics periodically from cache service.
     */
    @Scheduled(fixedRateString = "${kuber.prometheus.update-interval-ms:5000}")
    public void updateMetrics() {
        if (!initialized || !kuberProperties.getPrometheus().isEnabled()) {
            return;
        }

        try {
            // Update memory metrics
            Runtime runtime = Runtime.getRuntime();
            heapUsedBytes.set(runtime.totalMemory() - runtime.freeMemory());
            heapMaxBytes.set(runtime.maxMemory());

            // Update value cache metrics
            valueCacheLimit.set(kuberProperties.getCache().getValueCacheMaxEntries());

            // Update per-region metrics
            updateRegionMetrics();

        } catch (Exception e) {
            log.debug("Error updating Prometheus metrics: {}", e.getMessage());
        }
    }

    private void updateRegionMetrics() {
        try {
            Set<String> regions = cacheService.getRegionNames();
            long totalValueCacheSize = 0;

            for (String region : regions) {
                // Ensure region gauge exists
                if (!regionKeyCounts.containsKey(region)) {
                    registerRegionMetrics(region);
                }

                // Update region key count
                long keyCount = cacheService.getKeyCount(region);
                regionKeyCounts.computeIfAbsent(region, k -> new AtomicLong()).set(keyCount);

                // Update region memory estimation
                long memoryEst = estimateRegionMemory(region, keyCount);
                regionMemoryUsage.computeIfAbsent(region, k -> new AtomicLong()).set(memoryEst);

                totalValueCacheSize += cacheService.getValueCacheSize(region);
            }

            valueCacheSize.set(totalValueCacheSize);

        } catch (Exception e) {
            log.debug("Error updating region metrics: {}", e.getMessage());
        }
    }

    private void registerRegionMetrics(String region) {
        AtomicLong keyCount = new AtomicLong(0);
        regionKeyCounts.put(region, keyCount);

        Gauge.builder("kuber_region_keys", keyCount, AtomicLong::get)
                .description("Number of keys in region")
                .tag("application", "kuber")
                .tag("region", region)
                .register(meterRegistry);

        AtomicLong memoryUsage = new AtomicLong(0);
        regionMemoryUsage.put(region, memoryUsage);

        Gauge.builder("kuber_region_memory_bytes", memoryUsage, AtomicLong::get)
                .description("Estimated memory usage for region in bytes")
                .tag("application", "kuber")
                .tag("region", region)
                .register(meterRegistry);

        log.debug("Registered metrics for region: {}", region);
    }

    private long estimateRegionMemory(String region, long keyCount) {
        // Rough estimation: average 256 bytes per entry
        return keyCount * 256;
    }

    private double calculateHitRate() {
        long hits = totalHits.get();
        long total = hits + totalMisses.get();
        return total > 0 ? (double) hits / total : 0.0;
    }

    private double calculateHeapUsageRatio() {
        long max = heapMaxBytes.get();
        return max > 0 ? (double) heapUsedBytes.get() / max : 0.0;
    }

    private long getTotalKeyCount() {
        try {
            return cacheService.getRegionNames().stream()
                    .mapToLong(cacheService::getKeyCount)
                    .sum();
        } catch (Exception e) {
            return 0;
        }
    }

    private long getRegionCount() {
        try {
            return cacheService.getRegionNames().size();
        } catch (Exception e) {
            return 0;
        }
    }

    // Methods to record operations (called by CacheService)

    public void recordGet(boolean hit) {
        totalGets.incrementAndGet();
        if (hit) {
            totalHits.incrementAndGet();
        } else {
            totalMisses.incrementAndGet();
        }
    }

    public void recordSet() {
        totalSets.incrementAndGet();
    }

    public void recordDelete() {
        totalDeletes.incrementAndGet();
    }

    public void recordEviction() {
        totalEvictions.incrementAndGet();
    }

    public void recordExpiration() {
        totalExpirations.incrementAndGet();
    }

    public void recordGetLatency(long microseconds) {
        // Simple moving average
        avgGetLatencyMicros.set((avgGetLatencyMicros.get() + microseconds) / 2);
    }

    public void recordSetLatency(long microseconds) {
        // Simple moving average
        avgSetLatencyMicros.set((avgSetLatencyMicros.get() + microseconds) / 2);
    }

    public void updateRequestsPerSecond(long rps) {
        requestsPerSecond.set(rps);
    }

    // Getters for current values

    public long getTotalGets() {
        return totalGets.get();
    }

    public long getTotalSets() {
        return totalSets.get();
    }

    public long getTotalHits() {
        return totalHits.get();
    }

    public long getTotalMisses() {
        return totalMisses.get();
    }

    public double getHitRate() {
        return calculateHitRate();
    }

    public boolean isInitialized() {
        return initialized;
    }
}
