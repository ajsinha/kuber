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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for tracking cache metrics and activities for monitoring.
 * Maintains time-series data for cache operations per region.
 * 
 * <p>Skips metric rotation when:
 * <ul>
 *   <li>Cache service is not yet initialized (during startup/loading)</li>
 *   <li>System is shutting down</li>
 * </ul>
 * 
 * @version 2.5.0
 */
@Slf4j
@Service
public class CacheMetricsService {
    
    // Keep 60 minutes of per-minute metrics
    private static final int MAX_MINUTES_RETAINED = 60;
    
    // Per-region operation counters (current minute)
    private final Map<String, RegionMetrics> currentMetrics = new ConcurrentHashMap<>();
    
    // Historical per-minute metrics per region
    private final Map<String, Deque<MinuteMetrics>> historicalMetrics = new ConcurrentHashMap<>();
    
    // Global counters since server start
    private final AtomicLong totalGets = new AtomicLong(0);
    private final AtomicLong totalSets = new AtomicLong(0);
    private final AtomicLong totalDeletes = new AtomicLong(0);
    private final AtomicLong totalHits = new AtomicLong(0);
    private final AtomicLong totalMisses = new AtomicLong(0);
    
    // Shutdown flag to stop scheduled tasks
    private volatile boolean shuttingDown = false;
    
    // Reference to cache service to check initialization status
    private final CacheService cacheService;
    
    public CacheMetricsService(@Lazy CacheService cacheService) {
        this.cacheService = cacheService;
    }
    
    /**
     * Record a GET operation
     */
    public void recordGet(String region, boolean hit) {
        totalGets.incrementAndGet();
        if (hit) {
            totalHits.incrementAndGet();
        } else {
            totalMisses.incrementAndGet();
        }
        getOrCreateMetrics(region).gets.incrementAndGet();
        if (hit) {
            getOrCreateMetrics(region).hits.incrementAndGet();
        } else {
            getOrCreateMetrics(region).misses.incrementAndGet();
        }
    }
    
    /**
     * Record a SET/PUT operation
     */
    public void recordSet(String region) {
        totalSets.incrementAndGet();
        getOrCreateMetrics(region).sets.incrementAndGet();
    }
    
    /**
     * Record multiple SET/PUT operations (for batch writes)
     */
    public void recordSets(String region, int count) {
        totalSets.addAndGet(count);
        getOrCreateMetrics(region).sets.addAndGet(count);
    }
    
    /**
     * Record a DELETE operation
     */
    public void recordDelete(String region) {
        totalDeletes.incrementAndGet();
        getOrCreateMetrics(region).deletes.incrementAndGet();
    }
    
    /**
     * Record a SEARCH operation
     */
    public void recordSearch(String region) {
        getOrCreateMetrics(region).searches.incrementAndGet();
    }
    
    private RegionMetrics getOrCreateMetrics(String region) {
        return currentMetrics.computeIfAbsent(region, k -> new RegionMetrics());
    }
    
    /**
     * Shutdown the metrics service.
     * Stops scheduled tasks and performs final metric rotation.
     */
    public void shutdown() {
        log.info("Shutting down CacheMetricsService...");
        
        // Perform final metric rotation to capture any remaining data BEFORE setting shuttingDown
        doRotateMetrics();
        
        shuttingDown = true;
        
        log.info("CacheMetricsService shutdown complete");
    }
    
    /**
     * Check if service is shutting down.
     */
    public boolean isShuttingDown() {
        return shuttingDown;
    }
    
    /**
     * Scheduled task to rotate metrics every minute.
     * Skips execution when cache is loading or system is shutting down.
     */
    @Scheduled(cron = "0 * * * * *") // Every minute at :00
    public void rotateMetrics() {
        // Skip if shutting down
        if (shuttingDown) {
            log.debug("System is shutting down, skipping metrics rotation");
            return;
        }
        
        // Skip if cache service is not yet initialized (during startup/loading)
        if (cacheService == null || !cacheService.isInitialized()) {
            log.debug("Cache service not yet initialized, skipping metrics rotation");
            return;
        }
        
        // Ensure all existing regions are being tracked (even if no activity)
        initializeRegionMetrics();
        
        doRotateMetrics();
    }
    
    /**
     * Initialize metrics tracking for all existing regions.
     * Called during rotation to ensure all regions appear in metrics.
     */
    private void initializeRegionMetrics() {
        try {
            Set<String> regionNames = cacheService.getRegionNames();
            for (String region : regionNames) {
                currentMetrics.computeIfAbsent(region, k -> new RegionMetrics());
            }
        } catch (Exception e) {
            log.debug("Could not initialize region metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Register a region for metrics tracking.
     * Called when a new region is created.
     */
    public void registerRegion(String region) {
        currentMetrics.computeIfAbsent(region, k -> new RegionMetrics());
        log.debug("Registered region '{}' for metrics tracking", region);
    }
    
    /**
     * Internal method to perform the actual metric rotation.
     * Called by scheduled task and during shutdown.
     */
    private void doRotateMetrics() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        
        for (Map.Entry<String, RegionMetrics> entry : currentMetrics.entrySet()) {
            String region = entry.getKey();
            RegionMetrics metrics = entry.getValue();
            
            // Create snapshot for this minute
            MinuteMetrics snapshot = new MinuteMetrics();
            snapshot.timestamp = now.toEpochMilli();
            snapshot.gets = metrics.gets.getAndSet(0);
            snapshot.sets = metrics.sets.getAndSet(0);
            snapshot.deletes = metrics.deletes.getAndSet(0);
            snapshot.hits = metrics.hits.getAndSet(0);
            snapshot.misses = metrics.misses.getAndSet(0);
            snapshot.searches = metrics.searches.getAndSet(0);
            
            // Add to history
            Deque<MinuteMetrics> history = historicalMetrics.computeIfAbsent(
                region, k -> new ConcurrentLinkedDeque<>());
            history.addLast(snapshot);
            
            // Trim old entries
            while (history.size() > MAX_MINUTES_RETAINED) {
                history.removeFirst();
            }
        }
        
        log.debug("Rotated metrics for {} regions", currentMetrics.size());
    }
    
    /**
     * Get historical metrics for a region
     */
    public List<MinuteMetrics> getHistoricalMetrics(String region, int minutes) {
        Deque<MinuteMetrics> history = historicalMetrics.get(region);
        if (history == null || history.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<MinuteMetrics> result = new ArrayList<>(history);
        if (result.size() > minutes) {
            result = result.subList(result.size() - minutes, result.size());
        }
        return result;
    }
    
    /**
     * Get historical metrics for all regions
     */
    public Map<String, List<MinuteMetrics>> getAllHistoricalMetrics(int minutes) {
        Map<String, List<MinuteMetrics>> result = new LinkedHashMap<>();
        for (String region : historicalMetrics.keySet()) {
            result.put(region, getHistoricalMetrics(region, minutes));
        }
        return result;
    }
    
    /**
     * Get current minute metrics for a region
     */
    public Map<String, Long> getCurrentMetrics(String region) {
        RegionMetrics metrics = currentMetrics.get(region);
        if (metrics == null) {
            return Collections.emptyMap();
        }
        
        Map<String, Long> result = new LinkedHashMap<>();
        result.put("gets", metrics.gets.get());
        result.put("sets", metrics.sets.get());
        result.put("deletes", metrics.deletes.get());
        result.put("hits", metrics.hits.get());
        result.put("misses", metrics.misses.get());
        result.put("searches", metrics.searches.get());
        return result;
    }
    
    /**
     * Get global statistics
     */
    public Map<String, Long> getGlobalStats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("totalGets", totalGets.get());
        stats.put("totalSets", totalSets.get());
        stats.put("totalDeletes", totalDeletes.get());
        stats.put("totalHits", totalHits.get());
        stats.put("totalMisses", totalMisses.get());
        
        long total = totalHits.get() + totalMisses.get();
        if (total > 0) {
            stats.put("hitRatePercent", (totalHits.get() * 100) / total);
        } else {
            stats.put("hitRatePercent", 0L);
        }
        
        return stats;
    }
    
    /**
     * Get list of regions being tracked
     */
    public Set<String> getTrackedRegions() {
        Set<String> regions = new TreeSet<>();
        regions.addAll(currentMetrics.keySet());
        regions.addAll(historicalMetrics.keySet());
        return regions;
    }
    
    /**
     * Metrics for current minute (mutable)
     */
    private static class RegionMetrics {
        final AtomicLong gets = new AtomicLong(0);
        final AtomicLong sets = new AtomicLong(0);
        final AtomicLong deletes = new AtomicLong(0);
        final AtomicLong hits = new AtomicLong(0);
        final AtomicLong misses = new AtomicLong(0);
        final AtomicLong searches = new AtomicLong(0);
    }
    
    /**
     * Snapshot of one minute's metrics
     */
    @Data
    public static class MinuteMetrics {
        private long timestamp;
        private long gets;
        private long sets;
        private long deletes;
        private long hits;
        private long misses;
        private long searches;
        
        public long getTotal() {
            return gets + sets + deletes;
        }
    }
}
