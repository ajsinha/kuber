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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.kuber.core.constants.KuberConstants;
import com.kuber.core.exception.KuberException;
import com.kuber.core.exception.ReadOnlyException;
import com.kuber.core.exception.RegionException;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheEvent;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.model.KeyIndexEntry;
import com.kuber.core.model.KeyIndexEntry.ValueLocation;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.event.EventPublisher;
import com.kuber.server.persistence.PersistenceOperationLock;
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.replication.ReplicationManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Main cache service providing all cache operations.
 * Supports regions, TTL, JSON queries, and replication.
 * 
 * HYBRID MEMORY ARCHITECTURE (v1.2.1):
 * - All keys are ALWAYS kept in memory via KeyIndex (O(1) existence checks)
 * - Values can be in memory (hot) or on disk only (cold)
 * - EXISTS and KEYS operations NEVER hit disk
 * - When memory is constrained, only values are evicted (keys stay in index)
 * 
 * Performance characteristics:
 * - EXISTS: O(1) pure memory lookup - 100x faster than disk-based
 * - KEYS pattern: O(n) memory scan - no disk I/O ever
 * - GET (key exists, value in memory): O(1)
 * - GET (key exists, value on disk): O(1) index + disk read
 * - GET (key doesn't exist): O(1) - immediate return, no disk I/O
 * - DBSIZE: O(1) from index.size()
 * 
 * @version 1.4.1
 */
@Slf4j
@Service
public class CacheService {
    
    private static final String VERSION = "1.4.1";
    
    private final KuberProperties properties;
    private final PersistenceStore persistenceStore;
    private final EventPublisher eventPublisher;
    private final CacheMetricsService metricsService;
    private final PersistenceOperationLock operationLock;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // BackupRestoreService for region lock checking during restore
    private com.kuber.server.backup.BackupRestoreService backupRestoreService;
    
    // ==================== HYBRID MEMORY ARCHITECTURE ====================
    // Key Index: ALL keys always in memory for O(1) lookups (per region)
    // Key Index: ALL keys always in memory (per region) - can be on-heap or off-heap
    private final Map<String, KeyIndexInterface> keyIndices = new ConcurrentHashMap<>();
    
    // Value Cache: Hot values in memory (can be evicted to disk)
    private final Map<String, Cache<String, CacheEntry>> regionCaches = new ConcurrentHashMap<>();
    
    // Region metadata
    private final Map<String, CacheRegion> regions = new ConcurrentHashMap<>();
    
    // Effective memory limits per region (for value cache only - keys are always in memory)
    private final Map<String, Integer> effectiveMemoryLimits = new ConcurrentHashMap<>();
    
    // Statistics
    private final Map<String, Map<String, Long>> statistics = new ConcurrentHashMap<>();
    
    // Initialization state - prevents operations before recovery is complete
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    
    // Shutdown flag to stop scheduled tasks
    private volatile boolean shuttingDown = false;
    
    // Guard against double shutdown
    private volatile boolean alreadyShutdown = false;
    
    @Autowired(required = false)
    private com.kuber.server.publishing.RegionEventPublishingService publishingService;
    
    public CacheService(KuberProperties properties, 
                        PersistenceStore persistenceStore,
                        EventPublisher eventPublisher,
                        CacheMetricsService metricsService,
                        PersistenceOperationLock operationLock) {
        this.properties = properties;
        this.persistenceStore = persistenceStore;
        this.eventPublisher = eventPublisher;
        this.metricsService = metricsService;
        this.operationLock = operationLock;
    }
    
    /**
     * Initialize the cache service.
     * Called by StartupOrchestrator after Spring context is fully loaded.
     * This method recovers data from persistence store.
     */
    public void initialize() {
        if (initialized.get()) {
            log.warn("CacheService already initialized, skipping...");
            return;
        }
        log.info("Initializing Kuber cache service v{} with HYBRID MEMORY ARCHITECTURE...", VERSION);
        log.info("Using persistence store: {}", persistenceStore.getType());
        log.info("HYBRID MODE: All keys will be kept in memory; values can overflow to disk");
        
        // Load regions from persistence store
        loadRegions();
        
        // Ensure default region exists
        if (!regions.containsKey(KuberConstants.DEFAULT_REGION)) {
            createDefaultRegion();
        }
        
        // Calculate memory allocation for all regions (for value caches only)
        calculateMemoryAllocation();
        
        // Create key indices and value caches with calculated limits
        for (String regionName : regions.keySet()) {
            createCacheForRegion(regionName);
        }
        
        // Prime cache: load ALL keys into index, hot values into cache
        primeCacheHybrid();
        
        // Mark as initialized
        initialized.set(true);
        
        String writeMode = properties.getPersistence().isSyncIndividualWrites() 
                ? "SYNC (durable)" 
                : "ASYNC (fast, eventually consistent)";
        log.info("Cache service initialized with {} regions (HYBRID MODE, Individual writes: {})", 
                regions.size(), writeMode);
    }
    
    /**
     * Check if cache service has completed initialization.
     * Used by other services to wait for recovery to complete.
     */
    public boolean isInitialized() {
        return initialized.get();
    }
    
    private void loadRegions() {
        try {
            log.info("Loading regions from {} persistence store...", persistenceStore.getType());
            List<CacheRegion> savedRegions = persistenceStore.loadAllRegions();
            log.info("Found {} regions in persistence store", savedRegions.size());
            
            for (CacheRegion region : savedRegions) {
                log.info("Loading region '{}' from persistence store", region.getName());
                regions.put(region.getName(), region);
                // Note: Cache creation moved to initialize() after memory allocation calculation
            }
        } catch (Exception e) {
            log.error("Failed to load regions from persistence store: {}", e.getMessage(), e);
        }
    }
    
    private void createDefaultRegion() {
        CacheRegion defaultRegion = CacheRegion.createDefault();
        regions.put(KuberConstants.DEFAULT_REGION, defaultRegion);
        // Note: Cache creation moved to initialize() after memory allocation calculation
        
        try {
            persistenceStore.saveRegion(defaultRegion);
        } catch (Exception e) {
            log.warn("Failed to persist default region: {}", e.getMessage());
        }
    }
    
    /**
     * Calculate memory allocation for all regions.
     * Uses smart allocation logic considering:
     * 1. Per-region configured limits (or default)
     * 2. Global cap (if enabled)
     * 3. Proportional allocation based on persisted entry counts
     */
    private void calculateMemoryAllocation() {
        int globalMax = properties.getCache().getGlobalMaxMemoryEntries();
        int defaultLimit = properties.getCache().getMaxMemoryEntries();
        
        log.info("Calculating memory allocation: globalMax={}, defaultPerRegion={}", 
                globalMax > 0 ? globalMax : "unlimited", defaultLimit);
        
        // Step 1: Collect configured/default limits and persisted counts for each region
        Map<String, Integer> configuredLimits = new HashMap<>();
        Map<String, Long> persistedCounts = new HashMap<>();
        long totalPersistedCount = 0;
        
        for (String regionName : regions.keySet()) {
            int configuredLimit = properties.getCache().getMemoryLimitForRegion(regionName);
            configuredLimits.put(regionName, configuredLimit);
            
            try {
                long count = persistenceStore.countNonExpiredEntries(regionName);
                persistedCounts.put(regionName, count);
                totalPersistedCount += count;
            } catch (Exception e) {
                persistedCounts.put(regionName, 0L);
                log.warn("Could not get entry count for region '{}': {}", regionName, e.getMessage());
            }
        }
        
        // Step 2: Calculate effective limits
        if (globalMax <= 0) {
            // No global cap - use configured/default limits directly
            for (String regionName : regions.keySet()) {
                int limit = configuredLimits.get(regionName);
                effectiveMemoryLimits.put(regionName, limit);
                log.info("Region '{}': memory limit = {} (no global cap)", regionName, limit);
            }
        } else {
            // Global cap enabled - use smart allocation
            int totalConfigured = configuredLimits.values().stream().mapToInt(Integer::intValue).sum();
            
            if (totalConfigured <= globalMax) {
                // Total configured fits within global cap - use configured limits
                for (String regionName : regions.keySet()) {
                    int limit = configuredLimits.get(regionName);
                    effectiveMemoryLimits.put(regionName, limit);
                    log.info("Region '{}': memory limit = {} (within global cap)", regionName, limit);
                }
            } else {
                // Need to scale down - use smart proportional allocation
                log.info("Total configured ({}) exceeds global cap ({}), using smart allocation", 
                        totalConfigured, globalMax);
                
                // Allocate based on: 50% proportional to configured limit, 50% proportional to data size
                int remaining = globalMax;
                int regionCount = regions.size();
                int minPerRegion = Math.max(1000, globalMax / (regionCount * 10)); // Minimum 1000 or 10% fair share
                
                for (String regionName : regions.keySet()) {
                    int configuredLimit = configuredLimits.get(regionName);
                    long persistedCount = persistedCounts.getOrDefault(regionName, 0L);
                    
                    // Calculate proportional share
                    double configuredRatio = (double) configuredLimit / totalConfigured;
                    double dataRatio = totalPersistedCount > 0 
                            ? (double) persistedCount / totalPersistedCount 
                            : 1.0 / regionCount;
                    
                    // Weighted average: 50% config-based, 50% data-based
                    double combinedRatio = (configuredRatio + dataRatio) / 2.0;
                    
                    // Calculate allocation
                    int allocation = (int) Math.max(minPerRegion, Math.round(globalMax * combinedRatio));
                    
                    // Don't exceed configured limit even if we have room
                    allocation = Math.min(allocation, configuredLimit);
                    
                    // Don't exceed what's actually persisted (no point allocating more)
                    if (persistedCount > 0 && persistedCount < allocation) {
                        allocation = (int) Math.min(allocation, persistedCount + 1000); // Some headroom
                    }
                    
                    effectiveMemoryLimits.put(regionName, allocation);
                    remaining -= allocation;
                    
                    log.info("Region '{}': memory limit = {} (configured={}, persisted={}, globalCap={})", 
                            regionName, allocation, configuredLimit, persistedCount, globalMax);
                }
                
                // If we have remaining capacity, redistribute to regions that could use more
                if (remaining > 0) {
                    redistributeRemainingCapacity(remaining, configuredLimits, persistedCounts);
                }
            }
        }
        
        int totalAllocated = effectiveMemoryLimits.values().stream().mapToInt(Integer::intValue).sum();
        log.info("Memory allocation complete: {} total entries across {} regions", 
                totalAllocated, regions.size());
    }
    
    /**
     * Redistribute remaining capacity to regions that could benefit from more memory.
     */
    private void redistributeRemainingCapacity(int remaining, 
                                               Map<String, Integer> configuredLimits,
                                               Map<String, Long> persistedCounts) {
        // Find regions that have more persisted data than allocated memory
        List<String> needMoreMemory = new ArrayList<>();
        for (String regionName : regions.keySet()) {
            int allocated = effectiveMemoryLimits.get(regionName);
            int configured = configuredLimits.get(regionName);
            long persisted = persistedCounts.getOrDefault(regionName, 0L);
            
            if (allocated < configured && allocated < persisted) {
                needMoreMemory.add(regionName);
            }
        }
        
        if (!needMoreMemory.isEmpty() && remaining > 0) {
            int perRegion = remaining / needMoreMemory.size();
            for (String regionName : needMoreMemory) {
                int current = effectiveMemoryLimits.get(regionName);
                int configured = configuredLimits.get(regionName);
                int newLimit = Math.min(current + perRegion, configured);
                effectiveMemoryLimits.put(regionName, newLimit);
                log.debug("Redistributed {} extra entries to region '{}'", newLimit - current, regionName);
            }
        }
    }
    
    /**
     * Get the effective memory limit for a region.
     */
    public int getEffectiveMemoryLimit(String regionName) {
        return effectiveMemoryLimits.getOrDefault(regionName, 
                properties.getCache().getMemoryLimitForRegion(regionName));
    }
    
    private void createCacheForRegion(String regionName) {
        int memoryLimit = getEffectiveMemoryLimit(regionName);
        
        // Create KeyIndex for this region (holds ALL keys in memory)
        // Choose between on-heap or off-heap based on configuration
        KeyIndexInterface keyIndex;
        if (properties.getCache().isOffHeapKeyIndex()) {
            int initialSizeMb = properties.getCache().getOffHeapKeyIndexInitialSizeMb();
            int maxSizeMb = properties.getCache().getOffHeapKeyIndexMaxSizeMb();
            
            // Validate sizes
            if (initialSizeMb < 1) {
                log.warn("Off-heap initial size {}MB is too small, using 16MB", initialSizeMb);
                initialSizeMb = 16;
            }
            if (maxSizeMb < initialSizeMb) {
                log.warn("Off-heap max size {}MB is less than initial size {}MB, using {}MB", 
                        maxSizeMb, initialSizeMb, initialSizeMb * 2);
                maxSizeMb = initialSizeMb * 2;
            }
            
            // Use long arithmetic to support sizes > 2GB (segmented buffers in OffHeapKeyIndex)
            long initialBytes = (long) initialSizeMb * 1024L * 1024L;
            long maxBytes = (long) maxSizeMb * 1024L * 1024L;
            
            keyIndex = new OffHeapKeyIndex(regionName, initialBytes, maxBytes);
            log.info("Created OFF-HEAP key index for region '{}' (initial: {}MB, max: {}MB)", 
                    regionName, initialSizeMb, maxSizeMb);
        } else {
            keyIndex = new KeyIndex(regionName);
            log.debug("Created ON-HEAP key index for region '{}'", regionName);
        }
        keyIndices.put(regionName, keyIndex);
        
        // Create value cache with eviction listener
        // Use synchronous executor to avoid ForkJoinPool issues during shutdown
        Cache<String, CacheEntry> cache = Caffeine.newBuilder()
                .maximumSize(memoryLimit)
                .expireAfterWrite(24, TimeUnit.HOURS)
                .executor(Runnable::run)  // Synchronous execution - no ForkJoinPool
                .removalListener((String key, CacheEntry value, RemovalCause cause) -> {
                    // Skip removal listener work during shutdown
                    if (shuttingDown) {
                        return;
                    }
                    
                    // When value is evicted from memory, update KeyIndex to mark as DISK only
                    if (cause == RemovalCause.SIZE || cause == RemovalCause.EXPIRED) {
                        KeyIndexInterface idx = keyIndices.get(regionName);
                        if (idx != null && key != null) {
                            if (cause == RemovalCause.SIZE) {
                                // Value evicted due to size - still exists on disk
                                idx.updateLocation(key, ValueLocation.DISK);
                                log.debug("Value for '{}' evicted from memory in region '{}' (still on disk)", 
                                        key, regionName);
                            } else if (cause == RemovalCause.EXPIRED) {
                                // Entry expired - will be cleaned up by expiration service
                                recordStatistic(regionName, KuberConstants.STAT_EXPIRED);
                            }
                        }
                        recordStatistic(regionName, KuberConstants.STAT_EVICTED);
                    }
                })
                .recordStats()
                .build();
        
        regionCaches.put(regionName, cache);
        statistics.put(regionName, new ConcurrentHashMap<>());
        
        log.debug("Created {} key index and value cache for region '{}' (value cache limit: {})", 
                keyIndex.isOffHeap() ? "OFF-HEAP" : "ON-HEAP", regionName, memoryLimit);
    }
    
    /**
     * Prime cache with hybrid architecture:
     * 1. Load ALL keys into KeyIndex (regardless of memory limit)
     * 2. Load hot values (up to memory limit) into value cache
     * 
     * Note: Sequential loading only - parallel loading removed in v1.3.8 for stability.
     */
    private void primeCacheHybrid() {
        log.info("Priming HYBRID cache from {} persistence store...", persistenceStore.getType());
        log.info("  - ALL keys will be loaded into memory (KeyIndex)");
        log.info("  - Hot values will be loaded into value cache (up to memory limit per region)");
        log.info("  - Sequential loading (one region at a time)");
        
        long totalKeysLoaded = 0;
        long totalValuesLoaded = 0;
        
        for (String regionName : regions.keySet()) {
            long[] result = primeRegion(regionName);
            totalKeysLoaded += result[0];
            totalValuesLoaded += result[1];
        }
        
        logPrimingComplete(totalKeysLoaded, totalValuesLoaded);
    }
    
    /**
     * Prime a single region's cache.
     * Marks region as loading to block queries until complete.
     * @return array of [keysLoaded, valuesLoaded]
     */
    private long[] primeRegion(String regionName) {
        // Mark region as loading - queries should wait
        operationLock.markRegionLoading(regionName);
        
        try {
            KeyIndexInterface keyIndex = keyIndices.get(regionName);
            Cache<String, CacheEntry> valueCache = regionCaches.get(regionName);
            int valueMemoryLimit = getEffectiveMemoryLimit(regionName);
            
            if (keyIndex == null || valueCache == null) {
                log.warn("KeyIndex or value cache not found for region '{}', skipping priming", regionName);
                return new long[]{0, 0};
            }
            
            // Load ALL entries from persistence to populate KeyIndex
            // For value cache, only load up to memory limit
            List<CacheEntry> entries = persistenceStore.loadEntries(regionName, Integer.MAX_VALUE);
            
            int keysLoaded = 0;
            int valuesLoaded = 0;
            
            for (CacheEntry entry : entries) {
                if (!entry.isExpired()) {
                    // Always add key to index
                    boolean valueInMemory = valuesLoaded < valueMemoryLimit;
                    ValueLocation location = valueInMemory ? ValueLocation.BOTH : ValueLocation.DISK;
                    keyIndex.putFromCacheEntry(entry, location);
                    keysLoaded++;
                    
                    // Only add hot values to cache (up to limit)
                    if (valueInMemory) {
                        valueCache.put(entry.getKey(), entry);
                        valuesLoaded++;
                    }
                }
            }
            
            log.info("Region '{}': loaded {} keys into index, {} values into cache (limit: {})", 
                    regionName, keysLoaded, valuesLoaded, valueMemoryLimit);
            
            return new long[]{keysLoaded, valuesLoaded};
            
        } catch (Exception e) {
            log.warn("Failed to prime cache for region '{}': {}", regionName, e.getMessage());
            return new long[]{0, 0};
        } finally {
            // Mark region as loaded - queries can proceed
            operationLock.markRegionLoaded(regionName);
        }
    }
    
    /**
     * Log cache priming completion.
     */
    private void logPrimingComplete(long totalKeysLoaded, long totalValuesLoaded) {
        log.info("HYBRID cache priming complete:");
        log.info("  - Total keys in memory (KeyIndex): {}", totalKeysLoaded);
        log.info("  - Total values in memory (cache): {}", totalValuesLoaded);
        log.info("  - EXISTS/KEYS operations will NEVER hit disk");
    }
    
    // ==================== Region Operations ====================
    
    /**
     * Validate region name: alphanumeric and underscore only, at least one alphabetic character.
     * This ensures directory/file discovery is sufficient without metadata dependency.
     */
    public static boolean isValidRegionName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        // Must contain only alphanumeric and underscore
        if (!name.matches("^[a-zA-Z0-9_]+$")) {
            return false;
        }
        // Must contain at least one alphabetic character
        if (!name.matches(".*[a-zA-Z].*")) {
            return false;
        }
        return true;
    }
    
    public CacheRegion createRegion(String name, String description) {
        checkWriteAccess();
        
        // Validate region name
        if (!isValidRegionName(name)) {
            throw new IllegalArgumentException(
                "Invalid region name '" + name + "'. Region names must contain only alphanumeric characters " +
                "and underscores, with at least one alphabetic character.");
        }
        
        if (regions.containsKey(name)) {
            throw RegionException.alreadyExists(name);
        }
        
        // Generate collection name (same as region name since it's already validated)
        String collectionName = "kuber_" + name.toLowerCase();
        
        CacheRegion region = CacheRegion.builder()
                .name(name)
                .description(description)
                .collectionName(collectionName)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        
        regions.put(name, region);
        
        // Calculate memory limit for new region (considering global cap if enabled)
        calculateMemoryLimitForNewRegion(name);
        
        createCacheForRegion(name);
        
        // Always persist region metadata (regardless of persistentMode)
        try {
            log.info("Persisting region '{}' to {} persistence store", name, persistenceStore.getType());
            persistenceStore.saveRegion(region);
            log.info("Successfully persisted region '{}' to persistence store", name);
        } catch (Exception e) {
            log.error("Failed to persist region '{}' to persistence store: {}", name, e.getMessage(), e);
            // Don't throw - region is still usable in-memory
        }
        
        eventPublisher.publish(CacheEvent.regionCreated(name, properties.getNodeId()));
        
        log.info("Created region '{}' with memory limit {}", name, getEffectiveMemoryLimit(name));
        return region;
    }
    
    /**
     * Calculate memory limit for a newly created region.
     * If global cap is enabled and we're near capacity, may need to reduce other regions.
     */
    private void calculateMemoryLimitForNewRegion(String regionName) {
        int globalMax = properties.getCache().getGlobalMaxMemoryEntries();
        int configuredLimit = properties.getCache().getMemoryLimitForRegion(regionName);
        
        if (globalMax <= 0) {
            // No global cap - use configured/default limit
            effectiveMemoryLimits.put(regionName, configuredLimit);
            return;
        }
        
        // Check current total allocation
        int currentTotal = effectiveMemoryLimits.values().stream().mapToInt(Integer::intValue).sum();
        int available = globalMax - currentTotal;
        
        if (available >= configuredLimit) {
            // Enough room for full allocation
            effectiveMemoryLimits.put(regionName, configuredLimit);
        } else if (available > 0) {
            // Partial allocation available
            int minAllocation = Math.max(1000, configuredLimit / 10);
            int allocation = Math.max(minAllocation, available);
            effectiveMemoryLimits.put(regionName, allocation);
            log.warn("Region '{}' allocated {} entries (requested {}, global cap reached)", 
                    regionName, allocation, configuredLimit);
        } else {
            // No room - allocate minimum and log warning
            int minAllocation = Math.max(1000, globalMax / (regions.size() * 10));
            effectiveMemoryLimits.put(regionName, minAllocation);
            log.warn("Region '{}' allocated minimum {} entries (global cap {} fully utilized)", 
                    regionName, minAllocation, globalMax);
        }
    }
    
    public void deleteRegion(String name) {
        checkWriteAccess();
        
        CacheRegion region = regions.get(name);
        if (region == null) {
            throw RegionException.notFound(name);
        }
        
        if (region.isCaptive() || region.isDefault()) {
            throw RegionException.captive(name);
        }
        
        // Remove KeyIndex for this region
        keyIndices.remove(name);
        
        // Clear the value cache
        Cache<String, CacheEntry> cache = regionCaches.remove(name);
        if (cache != null) {
            cache.invalidateAll();
        }
        
        regions.remove(name);
        statistics.remove(name);
        
        // Delete from persistence store
        persistenceStore.deleteRegion(name);
        
        eventPublisher.publish(CacheEvent.regionDeleted(name, properties.getNodeId()));
        
        log.info("Deleted region: {} (KeyIndex and value cache cleared)", name);
    }
    
    public void purgeRegion(String name) {
        checkWriteAccess();
        
        CacheRegion region = regions.get(name);
        if (region == null) {
            throw RegionException.notFound(name);
        }
        
        // Clear KeyIndex for this region
        KeyIndexInterface keyIndex = keyIndices.get(name);
        if (keyIndex != null) {
            keyIndex.clear();
        }
        
        // Clear value cache
        Cache<String, CacheEntry> cache = regionCaches.get(name);
        if (cache != null) {
            cache.invalidateAll();
        }
        
        region.setEntryCount(0);
        region.setUpdatedAt(Instant.now());
        
        // Purge from persistence store
        persistenceStore.purgeRegion(name);
        
        eventPublisher.publish(CacheEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(CacheEvent.EventType.REGION_PURGED)
                .region(name)
                .timestamp(Instant.now())
                .sourceNodeId(properties.getNodeId())
                .build());
        
        log.info("Purged region: {} (KeyIndex and value cache cleared)", name);
    }
    
    public Collection<CacheRegion> getAllRegions() {
        // Enrich regions with counts from KeyIndex (source of truth for keys)
        for (CacheRegion region : regions.values()) {
            KeyIndexInterface keyIndex = keyIndices.get(region.getName());
            long keyCount = keyIndex != null ? keyIndex.size() : 0;
            long memoryValueCount = getMemoryEntryCount(region.getName());
            
            region.setMemoryEntryCount(memoryValueCount);
            region.setPersistenceEntryCount(keyCount); // KeyIndex = source of truth
            region.setEntryCount(keyCount);
        }
        return Collections.unmodifiableCollection(regions.values());
    }
    
    public CacheRegion getRegion(String name) {
        CacheRegion region = regions.get(name);
        if (region != null) {
            // Enrich with current counts from KeyIndex
            KeyIndexInterface keyIndex = keyIndices.get(name);
            long keyCount = keyIndex != null ? keyIndex.size() : 0;
            long memoryValueCount = getMemoryEntryCount(name);
            
            region.setMemoryEntryCount(memoryValueCount);
            region.setPersistenceEntryCount(keyCount);
            region.setEntryCount(keyCount);
        }
        return region;
    }
    
    public boolean regionExists(String name) {
        return regions.containsKey(name);
    }
    
    // ==================== Attribute Mapping Operations ====================
    
    /**
     * Set attribute mapping for a region.
     * Maps source attribute names to target attribute names for JSON transformation.
     * 
     * @param regionName Region name
     * @param mapping Map of source attribute name to target attribute name
     */
    public void setAttributeMapping(String regionName, Map<String, String> mapping) {
        checkWriteAccess();
        ensureRegionExists(regionName);
        
        CacheRegion region = regions.get(regionName);
        if (region == null) {
            throw RegionException.notFound(regionName);
        }
        
        region.setAttributeMapping(mapping != null ? new HashMap<>(mapping) : new HashMap<>());
        region.setUpdatedAt(Instant.now());
        
        // Persist the updated region
        try {
            persistenceStore.saveRegion(region);
            log.info("Updated attribute mapping for region '{}': {} mappings", 
                    regionName, mapping != null ? mapping.size() : 0);
        } catch (Exception e) {
            log.error("Failed to persist attribute mapping for region '{}': {}", 
                    regionName, e.getMessage());
        }
    }
    
    /**
     * Get attribute mapping for a region.
     * 
     * @param regionName Region name
     * @return Attribute mapping map, or empty map if none configured
     */
    public Map<String, String> getAttributeMapping(String regionName) {
        CacheRegion region = regions.get(regionName);
        if (region == null) {
            return Collections.emptyMap();
        }
        Map<String, String> mapping = region.getAttributeMapping();
        return mapping != null ? Collections.unmodifiableMap(mapping) : Collections.emptyMap();
    }
    
    /**
     * Clear attribute mapping for a region.
     * 
     * @param regionName Region name
     */
    public void clearAttributeMapping(String regionName) {
        setAttributeMapping(regionName, null);
    }
    
    /**
     * Apply attribute mapping to a JSON node.
     * Creates a new JSON object with renamed attributes based on the mapping.
     * 
     * @param regionName Region to get mapping from
     * @param jsonNode Original JSON node
     * @return Transformed JSON node with renamed attributes
     */
    public JsonNode applyAttributeMapping(String regionName, JsonNode jsonNode) {
        if (jsonNode == null || !jsonNode.isObject()) {
            return jsonNode;
        }
        
        Map<String, String> mapping = getAttributeMapping(regionName);
        if (mapping == null || mapping.isEmpty()) {
            return jsonNode;
        }
        
        return transformJsonWithMapping(jsonNode, mapping);
    }
    
    /**
     * Apply attribute mapping from an explicit mapping.
     * 
     * @param jsonNode Original JSON node
     * @param mapping Attribute mapping to apply
     * @return Transformed JSON node with renamed attributes
     */
    public JsonNode applyAttributeMapping(JsonNode jsonNode, Map<String, String> mapping) {
        if (jsonNode == null || !jsonNode.isObject() || mapping == null || mapping.isEmpty()) {
            return jsonNode;
        }
        return transformJsonWithMapping(jsonNode, mapping);
    }
    
    /**
     * Transform JSON node using the given mapping.
     * Supports nested object transformation recursively.
     */
    private JsonNode transformJsonWithMapping(JsonNode jsonNode, Map<String, String> mapping) {
        if (!jsonNode.isObject()) {
            return jsonNode;
        }
        
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String originalName = field.getKey();
            JsonNode value = field.getValue();
            
            // Get mapped name or use original
            String mappedName = mapping.getOrDefault(originalName, originalName);
            
            // Recursively transform nested objects
            if (value.isObject()) {
                result.set(mappedName, transformJsonWithMapping(value, mapping));
            } else if (value.isArray()) {
                // Transform array elements if they are objects
                com.fasterxml.jackson.databind.node.ArrayNode arrayNode = mapper.createArrayNode();
                for (JsonNode element : value) {
                    if (element.isObject()) {
                        arrayNode.add(transformJsonWithMapping(element, mapping));
                    } else {
                        arrayNode.add(element);
                    }
                }
                result.set(mappedName, arrayNode);
            } else {
                result.set(mappedName, value);
            }
        }
        
        return result;
    }
    
    // ==================== String Operations ====================
    
    public String get(String region, String key) {
        checkRegionNotBeingRestored(region);
        CacheEntry entry = getEntry(region, key);
        if (entry == null) {
            return null;
        }
        return entry.getStringValue();
    }
    
    public void set(String region, String key, String value) {
        set(region, key, value, -1);
    }
    
    public void set(String region, String key, String value, long ttlSeconds) {
        checkWriteAccess();
        checkRegionNotBeingRestored(region);
        ensureRegionExists(region);
        
        // Check if key exists (for insert vs update detection)
        boolean isUpdate = exists(region, key);
        
        Instant now = Instant.now();
        Instant expiresAt = ttlSeconds > 0 ? now.plusSeconds(ttlSeconds) : null;
        
        // Apply attribute mapping if value is JSON and region has mapping configured
        String finalValue = value;
        Map<String, String> mapping = getAttributeMapping(region);
        if (mapping != null && !mapping.isEmpty() && JsonUtils.isValidJson(value)) {
            finalValue = JsonUtils.applyAttributeMapping(value, mapping);
        }
        
        CacheEntry entry = CacheEntry.builder()
                .id(UUID.randomUUID().toString())
                .key(key)
                .region(region)
                .valueType(CacheEntry.ValueType.STRING)
                .stringValue(finalValue)
                .ttlSeconds(ttlSeconds)
                .createdAt(now)
                .updatedAt(now)
                .expiresAt(expiresAt)
                .build();
        
        putEntry(region, key, entry);
        
        eventPublisher.publish(CacheEvent.entrySet(region, key, finalValue, properties.getNodeId()));
        
        // Trigger async event publishing (Kafka/ActiveMQ)
        if (publishingService != null) {
            if (isUpdate) {
                publishingService.publishUpdate(region, key, finalValue, properties.getNodeId());
            } else {
                publishingService.publishInsert(region, key, finalValue, properties.getNodeId());
            }
        }
    }
    
    public boolean setNx(String region, String key, String value) {
        checkWriteAccess();
        ensureRegionExists(region);
        
        if (exists(region, key)) {
            return false;
        }
        
        set(region, key, value);
        return true;
    }
    
    public void setEx(String region, String key, String value, long ttlSeconds) {
        set(region, key, value, ttlSeconds);
    }
    
    public String getSet(String region, String key, String value) {
        String oldValue = get(region, key);
        set(region, key, value);
        return oldValue;
    }
    
    /**
     * Batch GET using hybrid architecture:
     * 1. Check KeyIndex for each key (O(1) per key)
     * 2. For existing keys, check value cache
     * 3. For cold values (key exists but value not in memory), load from disk
     */
    public List<String> mget(String region, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return new ArrayList<>();
        }
        
        ensureRegionExists(region);
        KeyIndexInterface keyIndex = keyIndices.get(region);
        Cache<String, CacheEntry> valueCache = regionCaches.get(region);
        CacheRegion regionMeta = regions.get(region);
        
        List<String> values = new ArrayList<>(keys.size());
        List<String> keysToLoadFromPersistence = new ArrayList<>();
        Map<Integer, String> keyPositionMap = new HashMap<>(); // Track positions for persistence-loaded keys
        
        // First pass: check KeyIndex and value cache
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            
            // HYBRID: Check KeyIndex first - if not there, key doesn't exist
            KeyIndexEntry indexEntry = keyIndex.get(key);
            if (indexEntry == null) {
                // Key not in index = doesn't exist anywhere (no disk lookup needed)
                regionMeta.recordMiss();
                recordStatistic(region, KuberConstants.STAT_MISSES);
                metricsService.recordGet(region, false);
                values.add(null);
                continue;
            }
            
            // Key exists! Check value cache
            CacheEntry entry = valueCache.getIfPresent(key);
            
            if (entry != null) {
                // Value in memory
                if (entry.isExpired()) {
                    // Handle expired entry - remove from both index and cache
                    keyIndex.remove(key);
                    valueCache.invalidate(key);
                    try {
                        persistenceStore.deleteEntry(region, key);
                    } catch (Exception e) {
                        log.warn("Failed to delete expired entry '{}': {}", key, e.getMessage());
                    }
                    recordStatistic(region, KuberConstants.STAT_EXPIRED);
                    metricsService.recordGet(region, false);
                    values.add(null);
                } else {
                    // Valid entry in memory - fast path
                    entry.recordAccess();
                    indexEntry.recordAccess();
                    regionMeta.recordHit();
                    recordStatistic(region, KuberConstants.STAT_HITS);
                    metricsService.recordGet(region, true);
                    values.add(entry.getStringValue());
                }
            } else {
                // Key exists in index but value not in memory - need to load from disk (cold value)
                keysToLoadFromPersistence.add(key);
                keyPositionMap.put(i, key);
                values.add(null); // Placeholder
            }
        }
        
        // Batch load cold values from persistence if needed
        if (!keysToLoadFromPersistence.isEmpty()) {
            Map<String, CacheEntry> loaded = persistenceStore.loadEntriesByKeys(region, keysToLoadFromPersistence);
            
            for (Map.Entry<Integer, String> posEntry : keyPositionMap.entrySet()) {
                int index = posEntry.getKey();
                String key = posEntry.getValue();
                CacheEntry entry = loaded.get(key);
                
                if (entry != null && !entry.isExpired()) {
                    // Found valid entry on disk - add to value cache
                    valueCache.put(key, entry);
                    keyIndex.updateLocation(key, ValueLocation.BOTH);
                    entry.recordAccess();
                    KeyIndexEntry idxEntry = keyIndex.getWithoutTracking(key);
                    if (idxEntry != null) idxEntry.recordAccess();
                    regionMeta.recordHit();
                    recordStatistic(region, KuberConstants.STAT_HITS);
                    metricsService.recordGet(region, true);
                    values.set(index, entry.getStringValue());
                } else {
                    // Key was in index but value not on disk (inconsistency) or expired
                    if (entry != null && entry.isExpired()) {
                        // Expired - clean up
                        keyIndex.remove(key);
                        try {
                            persistenceStore.deleteEntry(region, key);
                        } catch (Exception e) {
                            log.warn("Failed to delete expired entry '{}': {}", key, e.getMessage());
                        }
                        recordStatistic(region, KuberConstants.STAT_EXPIRED);
                    } else {
                        // Inconsistency - key in index but not on disk
                        keyIndex.remove(key);
                        log.warn("MGET: Key '{}' was in index but not on disk - removed from index", key);
                    }
                    regionMeta.recordMiss();
                    recordStatistic(region, KuberConstants.STAT_MISSES);
                    metricsService.recordGet(region, false);
                }
            }
        }
        
        return values;
    }
    
    public void mset(String region, Map<String, String> entries) {
        checkWriteAccess();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            set(region, entry.getKey(), entry.getValue());
        }
    }
    
    public long incr(String region, String key) {
        return incrBy(region, key, 1);
    }
    
    public long incrBy(String region, String key, long increment) {
        checkWriteAccess();
        ensureRegionExists(region);
        
        String value = get(region, key);
        long current = 0;
        if (value != null) {
            try {
                current = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new KuberException(KuberException.ErrorCode.WRONG_TYPE, 
                        "value is not an integer or out of range");
            }
        }
        
        long newValue = current + increment;
        set(region, key, String.valueOf(newValue));
        return newValue;
    }
    
    public long decr(String region, String key) {
        return incrBy(region, key, -1);
    }
    
    public long decrBy(String region, String key, long decrement) {
        return incrBy(region, key, -decrement);
    }
    
    public int append(String region, String key, String value) {
        checkWriteAccess();
        String current = get(region, key);
        String newValue = (current != null ? current : "") + value;
        set(region, key, newValue);
        return newValue.length();
    }
    
    public int strlen(String region, String key) {
        String value = get(region, key);
        return value != null ? value.length() : 0;
    }
    
    // ==================== Key Operations ====================
    
    public boolean delete(String region, String key) {
        checkWriteAccess();
        ensureRegionExists(region);
        
        KeyIndexInterface keyIndex = keyIndices.get(region);
        Cache<String, CacheEntry> valueCache = regionCaches.get(region);
        
        // Check KeyIndex first - if not there, key doesn't exist
        boolean existedInIndex = keyIndex.containsKey(key);
        
        // Remove from KeyIndex
        keyIndex.remove(key);
        
        // Remove from value cache
        valueCache.invalidate(key);
        
        // Remove from persistence
        boolean deletedFromPersistence = false;
        if (existedInIndex) {
            try {
                persistenceStore.deleteEntry(region, key);
                deletedFromPersistence = true;
            } catch (Exception e) {
                log.debug("Entry '{}' not found in persistence for region '{}'", key, region);
            }
        }
        
        if (existedInIndex || deletedFromPersistence) {
            recordStatistic(region, KuberConstants.STAT_DELETES);
            metricsService.recordDelete(region);
            eventPublisher.publish(CacheEvent.entryDeleted(region, key, properties.getNodeId()));
            
            // Trigger async event publishing (Kafka/ActiveMQ)
            if (publishingService != null) {
                publishingService.publishDelete(region, key, properties.getNodeId());
            }
            
            return true;
        }
        
        return false;
    }
    
    public long delete(String region, List<String> keys) {
        checkWriteAccess();
        long count = 0;
        for (String key : keys) {
            if (delete(region, key)) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Check if key exists - O(1) using KeyIndex only (NEVER hits disk).
     * This is the primary benefit of the hybrid architecture.
     */
    public boolean exists(String region, String key) {
        ensureRegionExists(region);
        
        // HYBRID: Pure memory lookup via KeyIndex - no disk I/O!
        KeyIndexInterface keyIndex = keyIndices.get(region);
        return keyIndex.containsKey(key);
    }
    
    public long exists(String region, List<String> keys) {
        ensureRegionExists(region);
        KeyIndexInterface keyIndex = keyIndices.get(region);
        return keys.stream().filter(keyIndex::containsKey).count();
    }
    
    public boolean expire(String region, String key, long ttlSeconds) {
        checkWriteAccess();
        
        CacheEntry entry = getEntry(region, key);
        if (entry == null) {
            return false;
        }
        
        entry.setTtlSeconds(ttlSeconds);
        entry.setExpiresAt(Instant.now().plusSeconds(ttlSeconds));
        entry.setUpdatedAt(Instant.now());
        
        putEntry(region, key, entry);
        return true;
    }
    
    public long ttl(String region, String key) {
        CacheEntry entry = getEntry(region, key);
        if (entry == null) {
            return -2;  // Key does not exist
        }
        return entry.getRemainingTtl();
    }
    
    public boolean persist(String region, String key) {
        checkWriteAccess();
        
        CacheEntry entry = getEntry(region, key);
        if (entry == null) {
            return false;
        }
        
        entry.setTtlSeconds(-1);
        entry.setExpiresAt(null);
        entry.setUpdatedAt(Instant.now());
        
        putEntry(region, key, entry);
        return true;
    }
    
    public String type(String region, String key) {
        CacheEntry entry = getEntry(region, key);
        if (entry == null) {
            return "none";
        }
        return entry.getValueType().name().toLowerCase();
    }
    
    /**
     * Get keys matching pattern - pure memory scan via KeyIndex (NEVER hits disk).
     * This is a major performance improvement for large datasets.
     */
    public Set<String> keys(String region, String pattern) {
        ensureRegionExists(region);
        
        // HYBRID: Pure memory scan via KeyIndex - no disk I/O!
        KeyIndexInterface keyIndex = keyIndices.get(region);
        List<String> matchingKeys = keyIndex.findKeysByPattern(pattern);
        return new HashSet<>(matchingKeys);
    }
    
    /**
     * Get keys with a limit - pure memory scan via KeyIndex (NEVER hits disk).
     */
    public Set<String> keys(String region, String pattern, int limit) {
        ensureRegionExists(region);
        
        // HYBRID: Pure memory scan via KeyIndex - no disk I/O!
        KeyIndexInterface keyIndex = keyIndices.get(region);
        List<String> matchingKeys = keyIndex.findKeysByPattern(pattern);
        
        if (limit > 0 && matchingKeys.size() > limit) {
            return new LinkedHashSet<>(matchingKeys.subList(0, limit));
        }
        return new LinkedHashSet<>(matchingKeys);
    }
    
    /**
     * Search keys by regex pattern and return matching key-value pairs.
     * This is different from keys() which uses glob pattern and returns only keys.
     * 
     * @param region the cache region
     * @param regexPattern a Java regex pattern to match keys
     * @return list of maps containing key, value, type, and ttl for each match
     */
    public List<Map<String, Object>> searchKeysByRegex(String region, String regexPattern) {
        return searchKeysByRegex(region, regexPattern, 1000);
    }
    
    /**
     * Search keys by regex pattern with limit.
     * Searches both in-memory cache and persistence store.
     * 
     * @param region the cache region
     * @param regexPattern a Java regex pattern to match keys
     * @param limit maximum number of results
     * @return list of maps containing key, value, type, and ttl for each match
     */
    public List<Map<String, Object>> searchKeysByRegex(String region, String regexPattern, int limit) {
        ensureRegionExists(region);
        
        Pattern pattern;
        try {
            pattern = Pattern.compile(regexPattern);
        } catch (Exception e) {
            throw new KuberException(KuberException.ErrorCode.INVALID_ARGUMENT, 
                    "Invalid regex pattern: " + e.getMessage());
        }
        
        List<Map<String, Object>> results = new ArrayList<>();
        Set<String> processedKeys = new HashSet<>();
        
        // First search in-memory cache (faster)
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        for (Map.Entry<String, CacheEntry> entry : cache.asMap().entrySet()) {
            if (results.size() >= limit) {
                break;
            }
            
            String key = entry.getKey();
            if (pattern.matcher(key).matches()) {
                CacheEntry cacheEntry = entry.getValue();
                
                // Skip expired entries
                if (cacheEntry.isExpired()) {
                    continue;
                }
                
                processedKeys.add(key);
                results.add(entryToResultMap(cacheEntry));
                recordStatistic(region, KuberConstants.STAT_HITS);
            }
        }
        
        // Then search persistence store if we need more results
        if (results.size() < limit) {
            // Get non-expired keys from persistence
            List<String> persistenceKeys = persistenceStore.getNonExpiredKeys(region, "*", limit * 2);
            
            for (String key : persistenceKeys) {
                if (results.size() >= limit) {
                    break;
                }
                
                // Skip if already processed from memory
                if (processedKeys.contains(key)) {
                    continue;
                }
                
                if (pattern.matcher(key).matches()) {
                    CacheEntry cacheEntry = persistenceStore.get(region, key);
                    if (cacheEntry != null && !cacheEntry.isExpired()) {
                        results.add(entryToResultMap(cacheEntry));
                        recordStatistic(region, KuberConstants.STAT_HITS);
                    }
                }
            }
        }
        
        return results;
    }
    
    /**
     * Convert a CacheEntry to a result map for search results.
     */
    private Map<String, Object> entryToResultMap(CacheEntry cacheEntry) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", cacheEntry.getKey());
        result.put("value", cacheEntry.getStringValue());
        result.put("type", cacheEntry.getValueType().name().toLowerCase());
        result.put("ttl", cacheEntry.getRemainingTtl());
        
        // Include JSON value if applicable
        if (cacheEntry.getValueType() == CacheEntry.ValueType.JSON && cacheEntry.getJsonValue() != null) {
            result.put("jsonValue", cacheEntry.getJsonValue());
        }
        
        return result;
    }
    
    public boolean rename(String region, String oldKey, String newKey) {
        checkWriteAccess();
        
        CacheEntry entry = getEntry(region, oldKey);
        if (entry == null) {
            throw new KuberException(KuberException.ErrorCode.NO_SUCH_KEY, "no such key");
        }
        
        entry.setKey(newKey);
        entry.setUpdatedAt(Instant.now());
        
        delete(region, oldKey);
        putEntry(region, newKey, entry);
        
        return true;
    }
    
    // ==================== JSON Operations ====================
    
    public void jsonSet(String region, String key, JsonNode value) {
        jsonSet(region, key, "$", value, -1);
    }
    
    public void jsonSet(String region, String key, String path, JsonNode value, long ttlSeconds) {
        checkWriteAccess();
        ensureRegionExists(region);
        
        Instant now = Instant.now();
        Instant expiresAt = ttlSeconds > 0 ? now.plusSeconds(ttlSeconds) : null;
        
        CacheEntry existing = getEntry(region, key);
        JsonNode finalValue;
        
        if (existing != null && existing.getJsonValue() != null && !"$".equals(path)) {
            // Update existing JSON at path
            finalValue = JsonUtils.setPath(existing.getJsonValue().deepCopy(), path, value);
        } else {
            // Apply attribute mapping for root-level JSON set operations
            finalValue = applyAttributeMapping(region, value);
        }
        
        CacheEntry entry = CacheEntry.builder()
                .id(existing != null ? existing.getId() : UUID.randomUUID().toString())
                .key(key)
                .region(region)
                .valueType(CacheEntry.ValueType.JSON)
                .jsonValue(finalValue)
                .stringValue(JsonUtils.toJson(finalValue))
                .ttlSeconds(ttlSeconds)
                .createdAt(existing != null ? existing.getCreatedAt() : now)
                .updatedAt(now)
                .expiresAt(expiresAt)
                .build();
        
        putEntry(region, key, entry);
    }
    
    public JsonNode jsonGet(String region, String key) {
        return jsonGet(region, key, "$");
    }
    
    public JsonNode jsonGet(String region, String key, String path) {
        CacheEntry entry = getEntry(region, key);
        if (entry == null || entry.getJsonValue() == null) {
            return null;
        }
        return JsonUtils.getPath(entry.getJsonValue(), path);
    }
    
    public boolean jsonDelete(String region, String key, String path) {
        checkWriteAccess();
        
        CacheEntry entry = getEntry(region, key);
        if (entry == null || entry.getJsonValue() == null) {
            return false;
        }
        
        if ("$".equals(path)) {
            return delete(region, key);
        }
        
        JsonNode updated = JsonUtils.deletePath(entry.getJsonValue().deepCopy(), path);
        entry.setJsonValue(updated);
        entry.setStringValue(JsonUtils.toJson(updated));
        entry.setUpdatedAt(Instant.now());
        
        putEntry(region, key, entry);
        return true;
    }
    
    public List<CacheEntry> jsonSearch(String region, String query) {
        return jsonSearch(region, query, 1000);
    }
    
    /**
     * Search JSON entries by query with limit.
     * Searches both in-memory cache and persistence store.
     */
    public List<CacheEntry> jsonSearch(String region, String query, int limit) {
        ensureRegionExists(region);
        
        List<JsonUtils.QueryCondition> conditions = JsonUtils.parseQuery(query);
        List<CacheEntry> results = new ArrayList<>();
        Set<String> processedKeys = new HashSet<>();
        
        // First search in-memory cache
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        for (CacheEntry entry : cache.asMap().values()) {
            if (results.size() >= limit) {
                break;
            }
            
            if (entry.getValueType() == CacheEntry.ValueType.JSON 
                    && entry.getJsonValue() != null 
                    && !entry.isExpired()
                    && JsonUtils.matchesAllQueries(entry.getJsonValue(), conditions)) {
                processedKeys.add(entry.getKey());
                results.add(entry);
            }
        }
        
        // Then search persistence store if we need more results
        if (results.size() < limit) {
            List<String> persistenceKeys = persistenceStore.getNonExpiredKeys(region, "*", limit * 2);
            
            for (String key : persistenceKeys) {
                if (results.size() >= limit) {
                    break;
                }
                
                // Skip if already processed from memory
                if (processedKeys.contains(key)) {
                    continue;
                }
                
                CacheEntry entry = persistenceStore.get(region, key);
                if (entry != null 
                        && entry.getValueType() == CacheEntry.ValueType.JSON 
                        && entry.getJsonValue() != null 
                        && !entry.isExpired()
                        && JsonUtils.matchesAllQueries(entry.getJsonValue(), conditions)) {
                    results.add(entry);
                }
            }
        }
        
        return results;
    }
    
    // ==================== Hash Operations ====================
    
    public void hset(String region, String key, String field, String value) {
        checkWriteAccess();
        ensureRegionExists(region);
        
        CacheEntry entry = getEntry(region, key);
        JsonNode current;
        
        if (entry != null && entry.getJsonValue() != null) {
            current = entry.getJsonValue().deepCopy();
        } else {
            current = JsonUtils.getObjectMapper().createObjectNode();
        }
        
        ((com.fasterxml.jackson.databind.node.ObjectNode) current).put(field, value);
        jsonSet(region, key, current);
    }
    
    public String hget(String region, String key, String field) {
        JsonNode value = jsonGet(region, key, "$." + field);
        return value != null ? value.asText() : null;
    }
    
    public Map<String, String> hgetall(String region, String key) {
        JsonNode json = jsonGet(region, key);
        if (json == null || !json.isObject()) {
            return Collections.emptyMap();
        }
        
        Map<String, String> result = new HashMap<>();
        json.fields().forEachRemaining(e -> 
            result.put(e.getKey(), e.getValue().asText()));
        return result;
    }
    
    public boolean hdel(String region, String key, String field) {
        return jsonDelete(region, key, "$." + field);
    }
    
    public boolean hexists(String region, String key, String field) {
        return hget(region, key, field) != null;
    }
    
    public Set<String> hkeys(String region, String key) {
        return hgetall(region, key).keySet();
    }
    
    public Collection<String> hvals(String region, String key) {
        return hgetall(region, key).values();
    }
    
    public int hlen(String region, String key) {
        return hgetall(region, key).size();
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Get entry using hybrid architecture:
     * 1. Check KeyIndex first (O(1) - if not in index, key doesn't exist)
     * 2. Check value cache (O(1) - hot values)
     * 3. Load from disk if value not in memory (cold values)
     */
    private CacheEntry getEntry(String region, String key) {
        ensureRegionExists(region);
        
        // Wait for region to finish loading if in progress
        if (operationLock.isRegionLoading(region)) {
            log.debug("Region '{}' is loading, waiting for completion before get...", region);
            if (!operationLock.waitForRegionReady(region, 30, TimeUnit.SECONDS)) {
                log.warn("Timeout waiting for region '{}' to load - returning null", region);
                return null;
            }
        }
        
        KeyIndexInterface keyIndex = keyIndices.get(region);
        Cache<String, CacheEntry> valueCache = regionCaches.get(region);
        
        // HYBRID FAST PATH: Check KeyIndex first - if not there, key doesn't exist
        KeyIndexEntry indexEntry = keyIndex.get(key);
        if (indexEntry == null) {
            // Key not in index = doesn't exist anywhere (no disk lookup needed!)
            regions.get(region).recordMiss();
            recordStatistic(region, KuberConstants.STAT_MISSES);
            metricsService.recordGet(region, false);
            return null;
        }
        
        // Key exists! Check if value is in memory cache
        CacheEntry entry = valueCache.getIfPresent(key);
        
        if (entry != null) {
            // Value in memory - fast path
            if (entry.isExpired()) {
                // Handle expired entry - remove from both index and cache
                handleExpiredEntry(region, key, keyIndex, valueCache);
                return null;
            }
            
            // Valid entry in memory
            entry.recordAccess();
            indexEntry.recordAccess();
            regions.get(region).recordHit();
            recordStatistic(region, KuberConstants.STAT_HITS);
            metricsService.recordGet(region, true);
            return entry;
        }
        
        // Value not in memory but key exists - load from disk (cold value)
        entry = persistenceStore.loadEntry(region, key);
        
        if (entry != null) {
            if (!entry.isExpired()) {
                // Found valid entry on disk - add to memory cache
                valueCache.put(key, entry);
                keyIndex.updateLocation(key, ValueLocation.BOTH);
                entry.recordAccess();
                indexEntry.recordAccess();
                regions.get(region).recordHit();
                recordStatistic(region, KuberConstants.STAT_HITS);
                metricsService.recordGet(region, true);
                return entry;
            } else {
                // Expired on disk - clean up
                handleExpiredEntry(region, key, keyIndex, valueCache);
            }
        } else {
            // Inconsistency: key in index but not on disk - clean up index
            keyIndex.remove(key);
            log.warn("Key '{}' was in index but not on disk - removed from index", key);
        }
        
        regions.get(region).recordMiss();
        recordStatistic(region, KuberConstants.STAT_MISSES);
        metricsService.recordGet(region, false);
        return null;
    }
    
    /**
     * Handle expired entry cleanup.
     */
    private void handleExpiredEntry(String region, String key, KeyIndexInterface keyIndex, Cache<String, CacheEntry> valueCache) {
        keyIndex.remove(key);
        valueCache.invalidate(key);
        try {
            persistenceStore.deleteEntry(region, key);
        } catch (Exception e) {
            log.warn("Failed to delete expired entry '{}' from persistence: {}", key, e.getMessage());
        }
        recordStatistic(region, KuberConstants.STAT_EXPIRED);
        metricsService.recordGet(region, false);
    }
    
    /**
     * Put entry using hybrid architecture:
     * 1. Update KeyIndex (always)
     * 2. Update value cache (always for new/updated values)
     * 3. Persist to disk (sync or async based on configuration)
     * 
     * Write Mode (kuber.persistence.sync-individual-writes):
     * - false (default): Async mode - memory updated first, disk write in background (faster)
     * - true: Sync mode - disk write completes before returning (more durable)
     */
    private void putEntry(String region, String key, CacheEntry entry) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        Cache<String, CacheEntry> valueCache = regionCaches.get(region);
        
        boolean syncWrites = properties.getPersistence().isSyncIndividualWrites();
        
        if (syncWrites) {
            // SYNC MODE: Save to disk FIRST (synchronously) before updating index
            // This prevents race condition where key is in index but not on disk
            // Slower but maximum durability
            persistenceStore.saveEntry(entry);
            
            // Now update KeyIndex with value location = BOTH (confirmed on disk)
            keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
            
            // Add to value cache
            valueCache.put(key, entry);
        } else {
            // ASYNC MODE (default): Update memory first, disk write in background
            // Faster performance, eventually consistent with disk
            // Risk: If crash before async write completes, entry is lost
            
            // Update KeyIndex - mark as BOTH (will be on disk soon)
            keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
            
            // Add to value cache - entry is immediately readable
            valueCache.put(key, entry);
            
            // Save to disk asynchronously (non-blocking)
            persistenceStore.saveEntryAsync(entry);
        }
        
        recordStatistic(region, KuberConstants.STAT_SETS);
        metricsService.recordSet(region);
    }
    
    /**
     * Batch put entries for better performance during bulk operations like autoload.
     * Entries are grouped by region and saved in batches to the persistence store.
     * 
     * @param entries list of cache entries to save
     * @return number of entries successfully saved
     */
    public int putEntriesBatch(List<CacheEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        
        checkWriteAccess();
        
        // Group entries by region
        Map<String, List<CacheEntry>> entriesByRegion = new LinkedHashMap<>();
        for (CacheEntry entry : entries) {
            entriesByRegion.computeIfAbsent(entry.getRegion(), k -> new ArrayList<>()).add(entry);
        }
        
        int savedCount = 0;
        
        for (Map.Entry<String, List<CacheEntry>> regionGroup : entriesByRegion.entrySet()) {
            String region = regionGroup.getKey();
            List<CacheEntry> regionEntries = regionGroup.getValue();
            
            // Ensure region exists
            ensureRegionExists(region);
            
            KeyIndexInterface keyIndex = keyIndices.get(region);
            Cache<String, CacheEntry> valueCache = regionCaches.get(region);
            
            if (keyIndex == null || valueCache == null) {
                log.warn("Region '{}' not properly initialized, skipping batch", region);
                continue;
            }
            
            try {
                // CRITICAL: Save to disk FIRST (batch write) before updating index
                persistenceStore.saveEntries(regionEntries);
                
                // Now update KeyIndex and value cache for all entries
                for (CacheEntry entry : regionEntries) {
                    keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
                    valueCache.put(entry.getKey(), entry);
                    savedCount++;
                }
                
                // Record statistics
                if (properties.getCache().isEnableStatistics()) {
                    statistics.computeIfAbsent(region, k -> new ConcurrentHashMap<>())
                            .merge(KuberConstants.STAT_SETS, (long) regionEntries.size(), Long::sum);
                }
                metricsService.recordSets(region, regionEntries.size());
                
            } catch (Exception e) {
                log.error("Failed to batch save {} entries to region '{}': {}", 
                        regionEntries.size(), region, e.getMessage(), e);
                // Continue with other regions
            }
        }
        
        return savedCount;
    }
    
    private void ensureRegionExists(String region) {
        if (!regions.containsKey(region)) {
            // Auto-create region
            createRegion(region, "Auto-created region");
        }
    }
    
    /**
     * Public method to ensure a region exists (for batch operations).
     * Creates the region if it doesn't exist.
     * 
     * @param region the region name
     */
    public void ensureRegionExistsPublic(String region) {
        ensureRegionExists(region);
    }
    
    /**
     * Get all region names.
     * 
     * @return Set of region names
     */
    public Set<String> getRegionNames() {
        return new HashSet<>(regions.keySet());
    }
    
    /**
     * Clear the in-memory caches for a region (KeyIndex and ValueCache).
     * Used during restore to purge memory before loading backup data.
     * 
     * @param region Region name
     */
    public void clearRegionCaches(String region) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        Cache<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex != null) {
            keyIndex.clear();
            log.debug("Cleared KeyIndex for region '{}'", region);
        }
        
        if (valueCache != null) {
            valueCache.invalidateAll();
            log.debug("Cleared ValueCache for region '{}'", region);
        }
    }
    
    /**
     * Load entries into the in-memory caches (KeyIndex and ValueCache).
     * Used during restore to populate memory from backup data.
     * 
     * @param region Region name
     * @param entries List of entries to load
     */
    public void loadEntriesIntoCache(String region, List<CacheEntry> entries) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        Cache<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            log.warn("Region '{}' caches not initialized, skipping cache loading", region);
            return;
        }
        
        for (CacheEntry entry : entries) {
            keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
            valueCache.put(entry.getKey(), entry);
        }
    }
    
    /**
     * Check if a region is currently being restored.
     * Operations on the region should be blocked during restore.
     * 
     * @param region Region name
     * @return true if region is being restored
     */
    public boolean isRegionBeingRestored(String region) {
        // Delegate to BackupRestoreService if available
        if (backupRestoreService != null) {
            return backupRestoreService.isRegionBeingRestored(region);
        }
        return false;
    }
    
    /**
     * Check if a region is currently being loaded during startup.
     * Backup operations should wait until loading completes.
     * 
     * @param region Region name
     * @return true if region is being loaded
     */
    public boolean isRegionLoading(String region) {
        return operationLock.isRegionLoading(region);
    }
    
    /**
     * Set the BackupRestoreService reference for region lock checking.
     */
    public void setBackupRestoreService(Object backupRestoreService) {
        this.backupRestoreService = (com.kuber.server.backup.BackupRestoreService) backupRestoreService;
    }
    
    private void checkWriteAccess() {
        if (replicationManager != null && !replicationManager.isPrimary()) {
            throw new ReadOnlyException();
        }
    }
    
    /**
     * Check if a region is being restored and throw exception if so.
     * Both read and write operations should be blocked during restore.
     */
    private void checkRegionNotBeingRestored(String region) {
        if (backupRestoreService != null && backupRestoreService.isRegionBeingRestored(region)) {
            throw new IllegalStateException("Region '" + region + "' is currently being restored. " +
                    "Please wait for restore to complete.");
        }
    }
    
    private void recordStatistic(String region, String stat) {
        if (properties.getCache().isEnableStatistics()) {
            statistics.computeIfAbsent(region, k -> new ConcurrentHashMap<>())
                    .merge(stat, 1L, Long::sum);
        }
    }
    
    private Pattern globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*':
                    regex.append(".*");
                    break;
                case '?':
                    regex.append(".");
                    break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                    regex.append("\\").append(c);
                    break;
                case '[':
                case ']':
                    regex.append(c);
                    break;
                default:
                    regex.append(c);
            }
        }
        regex.append("$");
        return Pattern.compile(regex.toString());
    }
    
    // ==================== Statistics ====================
    
    public Map<String, Object> getStatistics(String region) {
        Map<String, Object> stats = new HashMap<>();
        
        CacheRegion regionObj = regions.get(region);
        if (regionObj != null) {
            stats.put("region", region);
            stats.put("entryCount", regionObj.getEntryCount());
            stats.put("hitRatio", regionObj.getHitRatio());
        }
        
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        if (cache != null) {
            stats.put("memoryEntries", cache.estimatedSize());
            stats.put("cacheStats", cache.stats().toString());
        }
        
        Map<String, Long> regionStats = statistics.get(region);
        if (regionStats != null) {
            stats.putAll(regionStats);
        }
        
        return stats;
    }
    
    public Map<String, Object> getServerInfo() {
        Map<String, Object> info = new HashMap<>();
        
        info.put("nodeId", properties.getNodeId());
        info.put("version", VERSION);
        info.put("architecture", "HYBRID"); // v1.2.1+ hybrid memory architecture
        info.put("regionCount", regions.size());
        info.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        info.put("maxMemoryEntries", properties.getCache().getMaxMemoryEntries());
        info.put("persistentMode", properties.getCache().isPersistentMode());
        
        // Key index storage type (v1.2.2)
        boolean offHeapEnabled = properties.getCache().isOffHeapKeyIndex();
        info.put("keyIndexStorage", offHeapEnabled ? "OFF_HEAP" : "ON_HEAP");
        
        // HYBRID stats - KeyIndex is source of truth for key counts
        long totalKeys = keyIndices.values().stream()
                .mapToLong(KeyIndexInterface::size)
                .sum();
        info.put("totalEntries", totalKeys);
        info.put("keysInMemory", totalKeys); // All keys always in memory
        
        // Off-heap memory usage (v1.2.2)
        if (offHeapEnabled) {
            long offHeapBytes = keyIndices.values().stream()
                    .mapToLong(KeyIndexInterface::getOffHeapBytesUsed)
                    .sum();
            info.put("offHeapBytesUsed", offHeapBytes);
            info.put("offHeapMbUsed", String.format("%.2f", offHeapBytes / (1024.0 * 1024.0)));
        }
        
        // Value cache count (hot values in memory)
        long totalMemoryValues = regionCaches.values().stream()
                .mapToLong(Cache::estimatedSize)
                .sum();
        info.put("valuesInMemory", totalMemoryValues);
        
        // Cold values (on disk only)
        long valuesOnDiskOnly = keyIndices.values().stream()
                .mapToLong(idx -> idx.getKeysOnDiskOnly().size())
                .sum();
        info.put("valuesOnDiskOnly", valuesOnDiskOnly);
        
        return info;
    }
    
    /**
     * Get database size - O(1) using KeyIndex (NEVER hits disk).
     */
    public long dbSize(String region) {
        if (region == null || region.isEmpty()) {
            // Return total across all regions from KeyIndex - O(1) per region
            return keyIndices.values().stream()
                    .mapToLong(KeyIndexInterface::size)
                    .sum();
        }
        
        // Return exact count from KeyIndex - O(1)
        KeyIndexInterface keyIndex = keyIndices.get(region);
        return keyIndex != null ? keyIndex.size() : 0;
    }
    
    /**
     * Get the metrics service for monitoring.
     */
    public CacheMetricsService getMetricsService() {
        return metricsService;
    }
    
    /**
     * Get in-memory value count for a region (values in Caffeine cache).
     */
    public long getMemoryEntryCount(String region) {
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        return cache != null ? cache.estimatedSize() : 0;
    }
    
    /**
     * Get total key count for a region from KeyIndex - O(1).
     * This is the source of truth for entry counts in hybrid architecture.
     */
    public long getKeyIndexCount(String region) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        return keyIndex != null ? keyIndex.size() : 0;
    }
    
    /**
     * Get persistence store entry count for a region.
     * In hybrid architecture, this returns KeyIndex count (source of truth).
     */
    public long getPersistenceEntryCount(String region) {
        // In hybrid mode, KeyIndex is the source of truth
        return getKeyIndexCount(region);
    }
    
    /**
     * Get fast entry count for a region - O(1) from KeyIndex.
     */
    public long getEstimatedPersistenceEntryCount(String region) {
        // KeyIndex provides O(1) exact count
        return getKeyIndexCount(region);
    }
    
    /**
     * Get detailed region statistics including KeyIndex and value cache.
     */
    public Map<String, Object> getDetailedRegionStats(String region) {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("region", region);
        
        KeyIndexInterface keyIndex = keyIndices.get(region);
        long totalKeys = keyIndex != null ? keyIndex.size() : 0;
        long memoryValues = getMemoryEntryCount(region);
        long diskOnlyKeys = keyIndex != null ? keyIndex.getKeysOnDiskOnly().size() : 0;
        
        CacheRegion regionObj = regions.get(region);
        if (regionObj != null) {
            stats.put("hitRatio", String.format("%.2f", regionObj.getHitRatio()));
        }
        
        // HYBRID stats
        stats.put("entryCount", totalKeys);           // Total keys (from KeyIndex)
        stats.put("keysInMemory", totalKeys);         // All keys are always in memory
        stats.put("valuesInMemory", memoryValues);    // Hot values in cache
        stats.put("valuesOnDiskOnly", diskOnlyKeys);  // Cold values (key in index, value on disk)
        
        // Chart-compatible fields (for admin dashboard Region Population chart)
        stats.put("memoryEntries", memoryValues);     // For chart: values in memory cache
        stats.put("persistenceEntries", totalKeys);   // For chart: total keys (all persisted)
        
        // Index statistics
        if (keyIndex != null) {
            stats.put("indexHitRate", String.format("%.2f%%", keyIndex.getHitRate() * 100));
        }
        
        return stats;
    }
    
    /**
     * Get all region statistics for monitoring dashboard.
     */
    public List<Map<String, Object>> getAllRegionStats() {
        List<Map<String, Object>> allStats = new ArrayList<>();
        for (String regionName : regions.keySet()) {
            allStats.add(getDetailedRegionStats(regionName));
        }
        return allStats;
    }
    
    // ==================== Memory Management Eviction ====================
    
    /**
     * Evict values from memory to persistence store (HYBRID ARCHITECTURE).
     * Used by MemoryWatcherService to reduce heap usage under memory pressure.
     * 
     * In hybrid mode:
     * - Keys ALWAYS stay in KeyIndex (never evicted)
     * - Only values are evicted from memory
     * - KeyIndex is updated to mark values as DISK only
     * 
     * @param count Maximum number of values to evict
     * @return Actual number of values evicted
     */
    public int evictEntriesToPersistence(int count) {
        int totalEvicted = 0;
        
        // Iterate through regions and evict values (not keys!)
        for (Map.Entry<String, Cache<String, CacheEntry>> regionEntry : regionCaches.entrySet()) {
            if (totalEvicted >= count) {
                break;
            }
            
            String regionName = regionEntry.getKey();
            Cache<String, CacheEntry> valueCache = regionEntry.getValue();
            KeyIndexInterface keyIndex = keyIndices.get(regionName);
            
            // Get values to evict
            int regionEvictCount = Math.min(count - totalEvicted, (int) valueCache.estimatedSize());
            if (regionEvictCount <= 0) {
                continue;
            }
            
            List<String> keysToEvict = new ArrayList<>();
            List<CacheEntry> entriesToPersist = new ArrayList<>();
            
            // Collect entries to evict
            for (Map.Entry<String, CacheEntry> entry : valueCache.asMap().entrySet()) {
                if (keysToEvict.size() >= regionEvictCount) {
                    break;
                }
                
                CacheEntry cacheEntry = entry.getValue();
                
                // Skip expired entries (they will be cleaned up by TTL cleanup)
                if (cacheEntry.isExpired()) {
                    continue;
                }
                
                keysToEvict.add(entry.getKey());
                entriesToPersist.add(cacheEntry);
            }
            
            // Persist entries before evicting values from memory
            for (CacheEntry entry : entriesToPersist) {
                try {
                    persistenceStore.saveEntry(entry);
                } catch (Exception e) {
                    log.warn("Failed to persist entry '{}' during eviction: {}", 
                            entry.getKey(), e.getMessage());
                }
            }
            
            // Evict values from memory cache and update KeyIndex
            for (String key : keysToEvict) {
                valueCache.invalidate(key);
                
                // HYBRID: Update KeyIndex to mark value as DISK only (key stays in index!)
                if (keyIndex != null) {
                    keyIndex.updateLocation(key, ValueLocation.DISK);
                }
                
                totalEvicted++;
                recordStatistic(regionName, "evictions");
            }
            
            if (!keysToEvict.isEmpty()) {
                log.debug("Evicted {} values from region '{}' to disk (keys remain in index)", 
                        keysToEvict.size(), regionName);
            }
        }
        
        return totalEvicted;
    }
    
    /**
     * Get total number of entries across all regions in memory.
     */
    public long getTotalMemoryEntries() {
        return regionCaches.values().stream()
                .mapToLong(Cache::estimatedSize)
                .sum();
    }
    
    // ==================== TTL Cleanup ====================
    
    @Scheduled(fixedRateString = "${kuber.cache.ttl-cleanup-interval-seconds:60}000")
    public void cleanupExpiredEntries() {
        // Skip if cache service not yet initialized
        if (!initialized.get()) {
            return;
        }
        
        // Skip if shutting down
        if (shuttingDown) {
            return;
        }
        
        for (Map.Entry<String, Cache<String, CacheEntry>> entry : regionCaches.entrySet()) {
            String regionName = entry.getKey();
            Cache<String, CacheEntry> valueCache = entry.getValue();
            KeyIndexInterface keyIndex = keyIndices.get(regionName);
            
            List<String> expiredKeys = new ArrayList<>();
            
            // Check value cache for expired entries
            valueCache.asMap().forEach((key, cacheEntry) -> {
                if (cacheEntry.isExpired()) {
                    expiredKeys.add(key);
                }
            });
            
            // Also check KeyIndex for expired entries (cold values on disk only)
            if (keyIndex != null) {
                expiredKeys.addAll(keyIndex.findExpiredKeys());
            }
            
            // Remove duplicates
            Set<String> uniqueExpiredKeys = new HashSet<>(expiredKeys);
            
            for (String key : uniqueExpiredKeys) {
                // HYBRID: Remove from both KeyIndex and value cache
                if (keyIndex != null) {
                    keyIndex.remove(key);
                }
                valueCache.invalidate(key);
                
                // Delete from persistence store
                try {
                    persistenceStore.deleteEntry(regionName, key);
                } catch (Exception e) {
                    log.warn("Failed to delete expired entry '{}' from persistence store: {}", key, e.getMessage());
                }
                
                recordStatistic(regionName, KuberConstants.STAT_EXPIRED);
            }
            
            if (!uniqueExpiredKeys.isEmpty()) {
                log.info("Cleaned up {} expired entries from region '{}' (removed from index and cache)", 
                        uniqueExpiredKeys.size(), regionName);
            }
        }
    }
    
    // ==================== Graceful Shutdown ====================
    
    /**
     * Shutdown the cache service - persist all data and clean up resources.
     * 
     * <p>NOTE: This is called by ShutdownOrchestrator, NOT by @PreDestroy.
     * This ensures proper shutdown order with delays between phases.
     * 
     * <p>Shutdown sequence:
     * <ol>
     *   <li>Set shutdown flag to stop scheduled tasks and removal listeners</li>
     *   <li>Persist all regions metadata</li>
     *   <li>Persist all cache entries to disk</li>
     *   <li>Invalidate all Caffeine caches (no removal listener callbacks due to shutdown flag)</li>
     *   <li>Shutdown key indices (releases off-heap memory)</li>
     * </ol>
     */
    public void shutdown() {
        // Guard against double shutdown
        if (alreadyShutdown) {
            log.debug("CacheService shutdown already completed - skipping duplicate shutdown call");
            return;
        }
        alreadyShutdown = true;
        
        log.info("Shutting down Kuber cache service - persisting all data to {} persistence store...", 
                persistenceStore.getType());
        
        // Step 1: Set shutdown flag FIRST to stop:
        // - Scheduled tasks (cleanupExpiredEntries)
        // - Caffeine removal listeners
        // - Any new operations
        shuttingDown = true;
        
        // Wait briefly for any in-flight operations to see the shutdown flag
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        long totalEntries = 0;
        long totalRegions = 0;
        
        try {
            // Step 2: Persist all regions metadata
            log.info("  Step 2: Persisting {} regions metadata...", regions.size());
            for (CacheRegion region : regions.values()) {
                try {
                    region.setUpdatedAt(Instant.now());
                    persistenceStore.saveRegion(region);
                    totalRegions++;
                } catch (Exception e) {
                    log.error("Failed to persist region '{}': {}", region.getName(), e.getMessage());
                }
            }
            
            // Step 3: Persist all cache entries from each region
            log.info("  Step 3: Persisting cache entries from {} regions...", regionCaches.size());
            for (Map.Entry<String, Cache<String, CacheEntry>> entry : regionCaches.entrySet()) {
                String regionName = entry.getKey();
                Cache<String, CacheEntry> cache = entry.getValue();
                
                try {
                    // Get all entries from cache
                    List<CacheEntry> entries = new ArrayList<>(cache.asMap().values());
                    
                    if (!entries.isEmpty()) {
                        // Filter out expired entries
                        List<CacheEntry> validEntries = entries.stream()
                                .filter(e -> !e.isExpired())
                                .collect(Collectors.toList());
                        
                        // Batch save to persistence store
                        if (!validEntries.isEmpty()) {
                            persistenceStore.saveEntries(validEntries);
                            totalEntries += validEntries.size();
                            log.info("    Persisted {} entries from region '{}'", validEntries.size(), regionName);
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to persist entries for region '{}': {}", regionName, e.getMessage());
                }
            }
            
            // Step 4: Invalidate all Caffeine caches
            // This won't trigger removal listeners because shuttingDown=true
            log.info("  Step 4: Invalidating {} Caffeine caches...", regionCaches.size());
            for (Map.Entry<String, Cache<String, CacheEntry>> entry : regionCaches.entrySet()) {
                try {
                    entry.getValue().invalidateAll();
                    entry.getValue().cleanUp();  // Force synchronous cleanup
                } catch (Exception e) {
                    log.warn("Failed to invalidate cache for region '{}': {}", entry.getKey(), e.getMessage());
                }
            }
            regionCaches.clear();
            
            // Step 5: Shutdown key indices (releases off-heap memory if used)
            log.info("  Step 5: Shutting down {} key indices...", keyIndices.size());
            for (Map.Entry<String, KeyIndexInterface> entry : keyIndices.entrySet()) {
                try {
                    KeyIndexInterface keyIndex = entry.getValue();
                    if (keyIndex.isOffHeap()) {
                        log.info("    Releasing off-heap memory for region '{}' ({} bytes)", 
                                entry.getKey(), keyIndex.getOffHeapBytesUsed());
                    }
                    keyIndex.shutdown();
                } catch (Exception e) {
                    log.error("Failed to shutdown key index for region '{}': {}", 
                            entry.getKey(), e.getMessage());
                }
            }
            keyIndices.clear();
            
            log.info("Shutdown complete - persisted {} regions and {} entries to {} persistence store", 
                    totalRegions, totalEntries, persistenceStore.getType());
            
        } catch (Exception e) {
            log.error("Error during shutdown persistence: {}", e.getMessage(), e);
        }
    }
}
