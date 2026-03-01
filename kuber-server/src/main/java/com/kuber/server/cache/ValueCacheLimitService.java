/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.cache;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Value Cache Limit Service for count-based cache management.
 * 
 * <p>This service enforces per-region limits on the number of values kept in memory,
 * independent of the memory pressure-based eviction. It ensures that each region
 * doesn't exceed a configured limit based on:
 * <ul>
 *   <li>Percentage of total keys (valueCacheMaxPercent, default: 20%)</li>
 *   <li>Absolute count (valueCacheMaxEntries, default: 10,000)</li>
 * </ul>
 * 
 * <p>The effective limit is the LOWER of these two values. For example:
 * <ul>
 *   <li>Region with 100,000 keys, 20% limit = 20,000 max, but if maxEntries=10,000 → limit is 10,000</li>
 *   <li>Region with 1,000 keys, 20% limit = 200 max, maxEntries=10,000 → limit is 200</li>
 * </ul>
 * 
 * <p>This service runs separately from the Memory Watcher Service:
 * <ul>
 *   <li><b>Memory Watcher:</b> Responds to JVM heap pressure (reactive)</li>
 *   <li><b>Value Cache Limit:</b> Enforces count-based limits proactively</li>
 * </ul>
 * 
 * <p>Configuration properties:
 * <ul>
 *   <li>kuber.cache.value-cache-limit-enabled: Enable/disable (default: true)</li>
 *   <li>kuber.cache.value-cache-max-percent: Max % of keys to cache (default: 20)</li>
 *   <li>kuber.cache.value-cache-max-entries: Max entries per region (default: 10,000)</li>
 *   <li>kuber.cache.value-cache-limit-check-interval-ms: Check interval (default: 30,000)</li>
 * </ul>
 * 
 * @since 1.7.4
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "kuber.cache.value-cache-limit-enabled", havingValue = "true", matchIfMissing = true)
public class ValueCacheLimitService {
    
    private final KuberProperties properties;
    private final CacheService cacheService;
    
    // State tracking
    private final AtomicBoolean checkInProgress = new AtomicBoolean(false);
    private final AtomicLong totalEvictedEntries = new AtomicLong(0);
    private final AtomicLong evictionCycles = new AtomicLong(0);
    private final AtomicLong totalChecks = new AtomicLong(0);
    
    // Shutdown flag
    private volatile boolean shuttingDown = false;
    
    // Last activity tracking
    private volatile Instant lastCheckTime = null;
    private volatile Instant lastEvictionTime = null;
    private volatile long lastEvictionCount = 0;
    private volatile long lastEvictionDurationMs = 0;
    private volatile String lastActivityMessage = "Waiting for first check...";
    private volatile Map<String, RegionStats> lastRegionStats = new LinkedHashMap<>();
    
    public ValueCacheLimitService(KuberProperties properties, CacheService cacheService) {
        this.properties = properties;
        this.cacheService = cacheService;
    }
    
    /**
     * Shutdown the value cache limit service.
     */
    public void shutdown() {
        log.info("Shutting down Value Cache Limit Service...");
        shuttingDown = true;
        log.info("Value Cache Limit Service shutdown complete");
    }
    
    /**
     * Check if service is shutting down.
     */
    public boolean isShuttingDown() {
        return shuttingDown;
    }
    
    @PostConstruct
    public void initialize() {
        KuberProperties.Cache cacheProps = properties.getCache();
        log.info("Value Cache Limit Service initialized");
        log.info("  Enabled: true");
        log.info("  Max percent of keys: {}%", cacheProps.getValueCacheMaxPercent());
        log.info("  Max entries per region: {}", cacheProps.getValueCacheMaxEntries());
        log.info("  Check interval: {}ms", cacheProps.getValueCacheLimitCheckIntervalMs());
    }
    
    /**
     * Scheduled task to check and enforce value cache limits.
     */
    @Scheduled(fixedDelayString = "${kuber.cache.value-cache-limit-check-interval-ms:30000}")
    public void checkValueCacheLimits() {
        // Skip if shutting down
        if (shuttingDown) {
            return;
        }
        
        // Skip if cache service not yet initialized
        if (!cacheService.isInitialized()) {
            return;
        }
        
        // Skip if check is already in progress
        if (!checkInProgress.compareAndSet(false, true)) {
            return;
        }
        
        try {
            totalChecks.incrementAndGet();
            lastCheckTime = Instant.now();
            
            performLimitCheck();
            
        } finally {
            checkInProgress.set(false);
        }
    }
    
    /**
     * Check all regions and evict values that exceed the limit.
     */
    private void performLimitCheck() {
        KuberProperties.Cache cacheProps = properties.getCache();
        int maxPercent = cacheProps.getValueCacheMaxPercent();
        int maxEntries = cacheProps.getValueCacheMaxEntries();
        
        Instant startTime = Instant.now();
        long totalEvicted = 0;
        Map<String, RegionStats> regionStats = new LinkedHashMap<>();
        
        // Get all regions
        for (String region : cacheService.getRegionNames()) {
            try {
                RegionStats stats = checkRegionLimit(region, maxPercent, maxEntries);
                regionStats.put(region, stats);
                totalEvicted += stats.evicted;
            } catch (Exception e) {
                log.error("Error checking value cache limit for region '{}': {}", region, e.getMessage());
            }
        }
        
        // Update statistics
        Instant endTime = Instant.now();
        long durationMs = Duration.between(startTime, endTime).toMillis();
        
        if (totalEvicted > 0) {
            totalEvictedEntries.addAndGet(totalEvicted);
            evictionCycles.incrementAndGet();
            lastEvictionTime = endTime;
            lastEvictionCount = totalEvicted;
            lastEvictionDurationMs = durationMs;
            
            lastActivityMessage = String.format("Evicted %,d values in %dms to enforce count limits", 
                    totalEvicted, durationMs);
            log.info("Value cache limit check complete: evicted {} entries in {}ms", totalEvicted, durationMs);
        } else {
            lastActivityMessage = String.format("All regions within limits (checked %d regions)", regionStats.size());
        }
        
        lastRegionStats = regionStats;
    }
    
    /**
     * Check and enforce limit for a single region.
     */
    private RegionStats checkRegionLimit(String region, int maxPercent, int maxEntries) {
        // Get total keys in region
        long totalKeys = cacheService.getKeyCount(region);
        
        // Get current value cache size
        long valueCacheSize = cacheService.getValueCacheSize(region);
        
        // Calculate limit: lower of percentage-based and absolute limit
        long percentLimit = (totalKeys * maxPercent) / 100;
        long effectiveLimit = Math.min(percentLimit, maxEntries);
        
        // Ensure at least some minimum if there are keys
        if (totalKeys > 0 && effectiveLimit < 100) {
            effectiveLimit = Math.min(100, totalKeys);
        }
        
        long evicted = 0;
        
        // Check if we exceed the limit
        if (valueCacheSize > effectiveLimit) {
            long toEvict = valueCacheSize - effectiveLimit;
            
            log.info("Region '{}': value cache size {} exceeds limit {} ({}% of {} keys or {}, whichever lower), evicting {}",
                    region, valueCacheSize, effectiveLimit, maxPercent, totalKeys, maxEntries, toEvict);
            
            // Evict the excess entries
            evicted = cacheService.evictValuesFromRegion(region, (int) Math.min(toEvict, Integer.MAX_VALUE));
            
            log.debug("Region '{}': evicted {} values, new cache size: {}", 
                    region, evicted, cacheService.getValueCacheSize(region));
        }
        
        return new RegionStats(totalKeys, valueCacheSize, effectiveLimit, evicted);
    }
    
    /**
     * Calculate the effective limit for a region.
     */
    public long calculateEffectiveLimit(String region) {
        KuberProperties.Cache cacheProps = properties.getCache();
        int maxPercent = cacheProps.getValueCacheMaxPercent();
        int maxEntries = cacheProps.getValueCacheMaxEntries();
        
        long totalKeys = cacheService.getKeyCount(region);
        long percentLimit = (totalKeys * maxPercent) / 100;
        long effectiveLimit = Math.min(percentLimit, maxEntries);
        
        // Ensure at least some minimum if there are keys
        if (totalKeys > 0 && effectiveLimit < 100) {
            effectiveLimit = Math.min(100, totalKeys);
        }
        
        return effectiveLimit;
    }
    
    /**
     * Force a limit check (for testing or manual intervention).
     */
    public void forceLimitCheck() {
        if (checkInProgress.compareAndSet(false, true)) {
            try {
                log.info("Forced value cache limit check triggered");
                lastActivityMessage = "Forced limit check started...";
                performLimitCheck();
            } finally {
                checkInProgress.set(false);
            }
        } else {
            log.warn("Limit check already in progress, skipping forced check");
        }
    }
    
    /**
     * Check if limit check is currently in progress.
     */
    public boolean isCheckInProgress() {
        return checkInProgress.get();
    }
    
    /**
     * Get statistics about value cache limit service activity.
     */
    public ValueCacheLimitStats getStats() {
        KuberProperties.Cache cacheProps = properties.getCache();
        return new ValueCacheLimitStats(
                cacheProps.getValueCacheMaxPercent(),
                cacheProps.getValueCacheMaxEntries(),
                cacheProps.getValueCacheLimitCheckIntervalMs(),
                totalEvictedEntries.get(),
                evictionCycles.get(),
                totalChecks.get(),
                lastCheckTime,
                lastEvictionTime,
                lastEvictionCount,
                lastEvictionDurationMs,
                lastActivityMessage,
                checkInProgress.get(),
                new LinkedHashMap<>(lastRegionStats)
        );
    }
    
    /**
     * Statistics record for value cache limit service.
     */
    public record ValueCacheLimitStats(
            int maxPercent,
            int maxEntries,
            int checkIntervalMs,
            long totalEvictedEntries,
            long evictionCycles,
            long totalChecks,
            Instant lastCheckTime,
            Instant lastEvictionTime,
            long lastEvictionCount,
            long lastEvictionDurationMs,
            String lastActivityMessage,
            boolean checkInProgress,
            Map<String, RegionStats> regionStats
    ) {}
    
    /**
     * Statistics for a single region.
     */
    public record RegionStats(
            long totalKeys,
            long valueCacheSize,
            long effectiveLimit,
            long evicted
    ) {}
}
