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

import com.kuber.core.model.CacheEntry;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.PersistenceStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Warm Object Service for maintaining minimum in-memory objects per region.
 * 
 * <p>This service ensures that configured regions maintain a minimum number of
 * "warm" objects (values loaded in memory) for optimal read performance.
 * 
 * <p><b>How it works:</b>
 * <ul>
 *   <li>Periodically checks each configured region's warm object count</li>
 *   <li>If value cache size is below the configured minimum, loads more objects from disk</li>
 *   <li>Prioritizes loading recently accessed or recently created entries</li>
 *   <li>Works in coordination with eviction services to prevent thrashing</li>
 * </ul>
 * 
 * <p><b>Configuration:</b>
 * <pre>
 * kuber:
 *   cache:
 *     warm-objects-enabled: true
 *     warm-object-check-interval-ms: 60000
 *     warm-object-load-batch-size: 1000
 *     region-warm-object-counts:
 *       trade: 100000
 *       reference: 50000
 *       session: 10000
 * </pre>
 * 
 * <p><b>Interaction with other services:</b>
 * <ul>
 *   <li><b>ValueCacheLimitService:</b> Evicts values to stay under max limit</li>
 *   <li><b>WarmObjectService:</b> Loads values to meet minimum threshold</li>
 *   <li><b>MemoryWatcherService:</b> Evicts during heap pressure (takes priority)</li>
 * </ul>
 * 
 * <p>The warm object count serves as a "floor" while the value cache limit serves
 * as a "ceiling". If both are configured for a region:
 * <ul>
 *   <li>Warm objects = 100,000</li>
 *   <li>Value cache max = 50,000</li>
 *   <li>Result: Value cache max takes priority (cannot exceed ceiling)</li>
 * </ul>
 * 
 * @since 1.7.9
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "kuber.cache.warm-objects-enabled", havingValue = "true", matchIfMissing = true)
public class WarmObjectService {
    
    private final KuberProperties properties;
    private final CacheService cacheService;
    private final PersistenceStore persistenceStore;
    
    // State tracking
    private final AtomicBoolean checkInProgress = new AtomicBoolean(false);
    private final AtomicLong totalObjectsLoaded = new AtomicLong(0);
    private final AtomicLong loadCycles = new AtomicLong(0);
    private final AtomicLong totalChecks = new AtomicLong(0);
    
    // Shutdown flag
    private volatile boolean shuttingDown = false;
    
    // Last activity tracking
    private volatile Instant lastCheckTime = null;
    private volatile Instant lastLoadTime = null;
    private volatile long lastLoadCount = 0;
    private volatile long lastLoadDurationMs = 0;
    private volatile String lastActivityMessage = "Waiting for first check...";
    private volatile Map<String, RegionWarmStats> lastRegionStats = new LinkedHashMap<>();
    
    public WarmObjectService(KuberProperties properties, 
                             CacheService cacheService,
                             PersistenceStore persistenceStore) {
        this.properties = properties;
        this.cacheService = cacheService;
        this.persistenceStore = persistenceStore;
    }
    
    /**
     * Shutdown the warm object service.
     */
    public void shutdown() {
        log.info("Shutting down Warm Object Service...");
        shuttingDown = true;
        log.info("Warm Object Service shutdown complete");
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
        Map<String, Integer> warmCounts = cacheProps.getRegionWarmObjectCounts();
        
        log.info("Warm Object Service initialized");
        log.info("  Enabled: true");
        log.info("  Check interval: {}ms", cacheProps.getWarmObjectCheckIntervalMs());
        log.info("  Load batch size: {}", cacheProps.getWarmObjectLoadBatchSize());
        
        if (warmCounts.isEmpty()) {
            log.info("  Configured regions: none (service will idle until regions are configured)");
        } else {
            log.info("  Configured regions: {}", warmCounts.size());
            warmCounts.forEach((region, count) -> 
                log.info("    {}: {} warm objects", region, String.format("%,d", count)));
        }
    }
    
    /**
     * Scheduled task to check and maintain warm object counts.
     */
    @Scheduled(fixedDelayString = "${kuber.cache.warm-object-check-interval-ms:60000}")
    public void checkWarmObjects() {
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
            
            performWarmObjectCheck();
            
        } finally {
            checkInProgress.set(false);
        }
    }
    
    /**
     * Perform warm object check for all configured regions.
     */
    private void performWarmObjectCheck() {
        Map<String, Integer> warmCounts = properties.getCache().getRegionWarmObjectCounts();
        
        if (warmCounts.isEmpty()) {
            lastActivityMessage = "No regions configured for warm objects";
            return;
        }
        
        Instant startTime = Instant.now();
        long totalLoaded = 0;
        Map<String, RegionWarmStats> regionStats = new LinkedHashMap<>();
        
        for (Map.Entry<String, Integer> entry : warmCounts.entrySet()) {
            String region = entry.getKey();
            int targetWarmCount = entry.getValue();
            
            // Skip if target is 0 or negative
            if (targetWarmCount <= 0) {
                continue;
            }
            
            // Check if region exists
            if (!cacheService.regionExists(region)) {
                log.debug("Region '{}' configured for warm objects but does not exist", region);
                regionStats.put(region, new RegionWarmStats(0, 0, targetWarmCount, 0, "Region not found"));
                continue;
            }
            
            try {
                RegionWarmStats stats = maintainWarmObjectsForRegion(region, targetWarmCount);
                regionStats.put(region, stats);
                totalLoaded += stats.loaded;
            } catch (Exception e) {
                log.error("Error maintaining warm objects for region '{}': {}", region, e.getMessage());
                regionStats.put(region, new RegionWarmStats(0, 0, targetWarmCount, 0, "Error: " + e.getMessage()));
            }
        }
        
        // Update statistics
        Instant endTime = Instant.now();
        long durationMs = Duration.between(startTime, endTime).toMillis();
        
        if (totalLoaded > 0) {
            totalObjectsLoaded.addAndGet(totalLoaded);
            loadCycles.incrementAndGet();
            lastLoadTime = endTime;
            lastLoadCount = totalLoaded;
            lastLoadDurationMs = durationMs;
            
            lastActivityMessage = String.format("Loaded %,d warm objects in %dms across %d regions", 
                    totalLoaded, durationMs, warmCounts.size());
            log.info("Warm object check complete: loaded {} objects in {}ms", totalLoaded, durationMs);
        } else {
            lastActivityMessage = String.format("All %d configured regions at or above warm target", 
                    warmCounts.size());
        }
        
        lastRegionStats = regionStats;
    }
    
    /**
     * Maintain warm objects for a specific region.
     * 
     * @param region The region name
     * @param targetWarmCount The target number of warm objects
     * @return Statistics about the operation
     */
    private RegionWarmStats maintainWarmObjectsForRegion(String region, int targetWarmCount) {
        // Get current stats
        long totalKeys = cacheService.getKeyCount(region);
        long currentWarmCount = cacheService.getValueCacheSize(region);
        
        // Cap target at total keys in region
        int effectiveTarget = (int) Math.min(targetWarmCount, totalKeys);
        
        // Check if we need to load more objects
        long deficit = effectiveTarget - currentWarmCount;
        
        if (deficit <= 0) {
            // Already at or above target
            return new RegionWarmStats(totalKeys, currentWarmCount, effectiveTarget, 0, "At target");
        }
        
        log.info("Region '{}': current warm={}, target={}, deficit={}, loading...", 
                region, currentWarmCount, effectiveTarget, deficit);
        
        // Load objects in batches
        int batchSize = properties.getCache().getWarmObjectLoadBatchSize();
        long loaded = 0;
        
        try {
            loaded = loadWarmObjectsFromDisk(region, (int) Math.min(deficit, Integer.MAX_VALUE), batchSize);
            
            if (loaded > 0) {
                log.info("Region '{}': loaded {} warm objects (new total: {})", 
                        region, loaded, cacheService.getValueCacheSize(region));
            }
        } catch (Exception e) {
            log.error("Error loading warm objects for region '{}': {}", region, e.getMessage());
            return new RegionWarmStats(totalKeys, currentWarmCount, effectiveTarget, 0, "Load error: " + e.getMessage());
        }
        
        String status = loaded >= deficit ? "Target reached" : "Partial load (" + loaded + "/" + deficit + ")";
        return new RegionWarmStats(totalKeys, cacheService.getValueCacheSize(region), effectiveTarget, loaded, status);
    }
    
    /**
     * Load warm objects from disk for a region.
     * 
     * @param region The region name
     * @param count Number of objects to load
     * @param batchSize Batch size for loading
     * @return Number of objects actually loaded
     */
    private long loadWarmObjectsFromDisk(String region, int count, int batchSize) {
        long loaded = 0;
        int remaining = count;
        
        // Get keys that are not currently in the value cache
        List<String> coldKeys = cacheService.getColdKeys(region, count);
        
        if (coldKeys.isEmpty()) {
            log.debug("Region '{}': no cold keys available to warm up", region);
            return 0;
        }
        
        // Load in batches
        for (int i = 0; i < coldKeys.size() && remaining > 0; i += batchSize) {
            int end = Math.min(i + batchSize, coldKeys.size());
            List<String> batchKeys = coldKeys.subList(i, end);
            
            try {
                // Load entries from persistence store (returns Map<String, CacheEntry>)
                Map<String, CacheEntry> entries = persistenceStore.loadEntriesByKeys(region, batchKeys);
                
                for (CacheEntry entry : entries.values()) {
                    if (entry != null && !entry.isExpired()) {
                        // Put into value cache (this warms the object)
                        cacheService.warmObject(region, entry);
                        loaded++;
                        remaining--;
                    }
                }
            } catch (Exception e) {
                log.warn("Error loading batch of warm objects for region '{}': {}", region, e.getMessage());
            }
        }
        
        return loaded;
    }
    
    /**
     * Force a warm object check (for testing or manual intervention).
     */
    public void forceWarmObjectCheck() {
        if (checkInProgress.compareAndSet(false, true)) {
            try {
                log.info("Forced warm object check triggered");
                lastActivityMessage = "Forced warm object check started...";
                performWarmObjectCheck();
            } finally {
                checkInProgress.set(false);
            }
        } else {
            log.warn("Warm object check already in progress, skipping forced check");
        }
    }
    
    /**
     * Get the target warm object count for a region.
     * 
     * @param region The region name
     * @return The target count, or 0 if not configured
     */
    public int getTargetWarmCount(String region) {
        return properties.getCache().getWarmObjectCountForRegion(region);
    }
    
    /**
     * Check if warm object maintenance is configured for a region.
     * 
     * @param region The region name
     * @return true if the region has a warm object configuration
     */
    public boolean isWarmObjectConfigured(String region) {
        return properties.getCache().hasWarmObjectConfig(region);
    }
    
    /**
     * Check if warm object check is currently in progress.
     */
    public boolean isCheckInProgress() {
        return checkInProgress.get();
    }
    
    /**
     * Get statistics about warm object service activity.
     */
    public WarmObjectStats getStats() {
        KuberProperties.Cache cacheProps = properties.getCache();
        return new WarmObjectStats(
                cacheProps.getWarmObjectCheckIntervalMs(),
                cacheProps.getWarmObjectLoadBatchSize(),
                new LinkedHashMap<>(cacheProps.getRegionWarmObjectCounts()),
                totalObjectsLoaded.get(),
                loadCycles.get(),
                totalChecks.get(),
                lastCheckTime,
                lastLoadTime,
                lastLoadCount,
                lastLoadDurationMs,
                lastActivityMessage,
                checkInProgress.get(),
                new LinkedHashMap<>(lastRegionStats)
        );
    }
    
    /**
     * Statistics record for warm object service.
     */
    public record WarmObjectStats(
            int checkIntervalMs,
            int loadBatchSize,
            Map<String, Integer> configuredRegions,
            long totalObjectsLoaded,
            long loadCycles,
            long totalChecks,
            Instant lastCheckTime,
            Instant lastLoadTime,
            long lastLoadCount,
            long lastLoadDurationMs,
            String lastActivityMessage,
            boolean checkInProgress,
            Map<String, RegionWarmStats> regionStats
    ) {}
    
    /**
     * Statistics for a single region's warm objects.
     */
    public record RegionWarmStats(
            long totalKeys,
            long currentWarmCount,
            int targetWarmCount,
            long loaded,
            String status
    ) {}
}
