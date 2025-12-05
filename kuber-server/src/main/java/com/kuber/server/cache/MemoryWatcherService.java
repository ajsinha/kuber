/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 * Patent Pending: Certain architectural patterns and implementations described in
 * this module may be subject to patent applications.
 */
package com.kuber.server.cache;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory Watcher Service for automated heap management.
 * 
 * This service monitors JVM heap usage and automatically evicts cache entries
 * to persistence when memory pressure is detected. This ensures the cache server
 * can run 24x7 without unexpected out-of-memory crashes.
 * 
 * When heap usage exceeds the high watermark (default 85%), entries are evicted
 * in batches (default 1000) until usage drops below the low watermark (default 50%).
 * 
 * Evicted entries are persisted to the backing store before removal from memory,
 * ensuring no data loss.
 * 
 * Configuration:
 * - kuber.cache.memory-watcher-enabled: Enable/disable the watcher (default: true)
 * - kuber.cache.memory-watcher-interval-ms: Check interval in milliseconds (default: 5000)
 * - kuber.cache.memory-high-watermark-percent: High watermark threshold (default: 85)
 * - kuber.cache.memory-low-watermark-percent: Low watermark threshold (default: 50)
 * - kuber.cache.memory-eviction-batch-size: Entries per eviction batch (default: 1000)
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "kuber.cache.memory-watcher-enabled", havingValue = "true", matchIfMissing = true)
public class MemoryWatcherService {
    
    private final KuberProperties properties;
    private final CacheService cacheService;
    private final MemoryMXBean memoryBean;
    
    // State tracking
    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);
    private final AtomicLong totalEvictedEntries = new AtomicLong(0);
    private final AtomicLong evictionCycles = new AtomicLong(0);
    private final AtomicLong totalChecks = new AtomicLong(0);
    
    // Last activity tracking
    private volatile Instant lastCheckTime = null;
    private volatile double lastCheckHeapPercent = 0;
    private volatile Instant lastEvictionTime = null;
    private volatile long lastEvictionCount = 0;
    private volatile long lastEvictionDurationMs = 0;
    private volatile double lastEvictionHeapBefore = 0;
    private volatile double lastEvictionHeapAfter = 0;
    private volatile String lastActivityMessage = "Waiting for first check...";
    
    public MemoryWatcherService(KuberProperties properties, CacheService cacheService) {
        this.properties = properties;
        this.cacheService = cacheService;
        this.memoryBean = ManagementFactory.getMemoryMXBean();
    }
    
    @PostConstruct
    public void initialize() {
        KuberProperties.Cache cacheProps = properties.getCache();
        log.info("Memory Watcher Service initialized");
        log.info("  Enabled: true");
        log.info("  Check interval: {}ms", cacheProps.getMemoryWatcherIntervalMs());
        log.info("  High watermark: {}%", cacheProps.getMemoryHighWatermarkPercent());
        log.info("  Low watermark: {}%", cacheProps.getMemoryLowWatermarkPercent());
        log.info("  Eviction batch size: {}", cacheProps.getMemoryEvictionBatchSize());
    }
    
    /**
     * Scheduled memory check task.
     * Runs at the configured interval to monitor heap usage.
     */
    @Scheduled(fixedDelayString = "${kuber.cache.memory-watcher-interval-ms:5000}")
    public void checkMemoryUsage() {
        // Skip if cache service not yet initialized
        if (!cacheService.isInitialized()) {
            return;
        }
        
        // Skip if eviction is already in progress
        if (!evictionInProgress.compareAndSet(false, true)) {
            return;
        }
        
        try {
            totalChecks.incrementAndGet();
            lastCheckTime = Instant.now();
            double heapUsagePercent = getHeapUsagePercent();
            lastCheckHeapPercent = heapUsagePercent;
            
            int highWatermark = properties.getCache().getMemoryHighWatermarkPercent();
            
            if (heapUsagePercent >= highWatermark) {
                lastActivityMessage = String.format("Heap at %.1f%% exceeds %d%%, evicting...", 
                        heapUsagePercent, highWatermark);
                log.warn("Heap usage {}% exceeds high watermark {}%, starting eviction...",
                        String.format("%.1f", heapUsagePercent), highWatermark);
                performEviction();
            } else {
                lastActivityMessage = String.format("Heap at %.1f%% is below %d%%, no action needed", 
                        heapUsagePercent, highWatermark);
            }
        } finally {
            evictionInProgress.set(false);
        }
    }
    
    /**
     * Perform eviction until heap usage drops below low watermark.
     */
    private void performEviction() {
        KuberProperties.Cache cacheProps = properties.getCache();
        int lowWatermark = cacheProps.getMemoryLowWatermarkPercent();
        int batchSize = cacheProps.getMemoryEvictionBatchSize();
        
        long totalEvicted = 0;
        int iterations = 0;
        int maxIterations = 100; // Safety limit to prevent infinite loops
        
        Instant startTime = Instant.now();
        double heapBefore = getHeapUsagePercent();
        lastEvictionHeapBefore = heapBefore;
        
        while (iterations < maxIterations) {
            double currentUsage = getHeapUsagePercent();
            
            if (currentUsage < lowWatermark) {
                log.info("Heap usage {}% is below low watermark {}%, eviction complete",
                        String.format("%.1f", currentUsage), lowWatermark);
                break;
            }
            
            // Evict a batch of entries
            int evicted = cacheService.evictEntriesToPersistence(batchSize);
            
            if (evicted == 0) {
                log.warn("No more entries to evict, but heap usage still at {}%",
                        String.format("%.1f", currentUsage));
                // Try to trigger GC to reclaim memory
                System.gc();
                break;
            }
            
            totalEvicted += evicted;
            iterations++;
            
            // Log progress every 5 iterations
            if (iterations % 5 == 0) {
                log.info("Eviction progress: {} entries evicted, heap at {}%",
                        totalEvicted, String.format("%.1f", getHeapUsagePercent()));
            }
            
            // Suggest GC after each batch to help reclaim memory
            if (iterations % 3 == 0) {
                System.gc();
            }
        }
        
        // Update statistics
        Instant endTime = Instant.now();
        double heapAfter = getHeapUsagePercent();
        
        totalEvictedEntries.addAndGet(totalEvicted);
        evictionCycles.incrementAndGet();
        lastEvictionTime = endTime;
        lastEvictionCount = totalEvicted;
        lastEvictionDurationMs = Duration.between(startTime, endTime).toMillis();
        lastEvictionHeapAfter = heapAfter;
        
        lastActivityMessage = String.format("Evicted %,d entries in %dms, heap: %.1f%% → %.1f%%",
                totalEvicted, lastEvictionDurationMs, heapBefore, heapAfter);
        
        log.info("Eviction cycle complete: evicted {} entries in {} iterations ({} ms), heap: {}% → {}%",
                totalEvicted, iterations, lastEvictionDurationMs, 
                String.format("%.1f", heapBefore), String.format("%.1f", heapAfter));
    }
    
    /**
     * Get current heap usage percentage.
     */
    public double getHeapUsagePercent() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        long used = heapUsage.getUsed();
        long max = heapUsage.getMax();
        
        if (max <= 0) {
            // If max is not defined, use committed
            max = heapUsage.getCommitted();
        }
        
        return (used * 100.0) / max;
    }
    
    /**
     * Get current heap usage in bytes.
     */
    public long getHeapUsedBytes() {
        return memoryBean.getHeapMemoryUsage().getUsed();
    }
    
    /**
     * Get max heap in bytes.
     */
    public long getHeapMaxBytes() {
        return memoryBean.getHeapMemoryUsage().getMax();
    }
    
    /**
     * Force an eviction cycle (for testing or manual intervention).
     */
    public void forceEviction() {
        if (evictionInProgress.compareAndSet(false, true)) {
            try {
                log.info("Forced eviction triggered");
                lastActivityMessage = "Forced eviction started...";
                performEviction();
            } finally {
                evictionInProgress.set(false);
            }
        } else {
            log.warn("Eviction already in progress, skipping forced eviction");
        }
    }
    
    /**
     * Check if eviction is currently in progress.
     */
    public boolean isEvictionInProgress() {
        return evictionInProgress.get();
    }
    
    /**
     * Get the configured check interval in milliseconds.
     */
    public int getCheckIntervalMs() {
        return properties.getCache().getMemoryWatcherIntervalMs();
    }
    
    /**
     * Get statistics about memory watcher activity.
     */
    public MemoryWatcherStats getStats() {
        return new MemoryWatcherStats(
                getHeapUsagePercent(),
                getHeapUsedBytes(),
                getHeapMaxBytes(),
                properties.getCache().getMemoryHighWatermarkPercent(),
                properties.getCache().getMemoryLowWatermarkPercent(),
                properties.getCache().getMemoryEvictionBatchSize(),
                properties.getCache().getMemoryWatcherIntervalMs(),
                totalEvictedEntries.get(),
                evictionCycles.get(),
                totalChecks.get(),
                lastCheckTime,
                lastCheckHeapPercent,
                lastEvictionTime,
                lastEvictionCount,
                lastEvictionDurationMs,
                lastEvictionHeapBefore,
                lastEvictionHeapAfter,
                lastActivityMessage,
                evictionInProgress.get()
        );
    }
    
    /**
     * Statistics record for memory watcher.
     */
    public record MemoryWatcherStats(
            double heapUsagePercent,
            long heapUsedBytes,
            long heapMaxBytes,
            int highWatermarkPercent,
            int lowWatermarkPercent,
            int evictionBatchSize,
            int checkIntervalMs,
            long totalEvictedEntries,
            long evictionCycles,
            long totalChecks,
            Instant lastCheckTime,
            double lastCheckHeapPercent,
            Instant lastEvictionTime,
            long lastEvictionCount,
            long lastEvictionDurationMs,
            double lastEvictionHeapBefore,
            double lastEvictionHeapAfter,
            String lastActivityMessage,
            boolean evictionInProgress
    ) {
        public long heapUsedMB() {
            return heapUsedBytes / (1024 * 1024);
        }
        
        public long heapMaxMB() {
            return heapMaxBytes / (1024 * 1024);
        }
    }
}
