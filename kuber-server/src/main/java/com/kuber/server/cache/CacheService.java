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
import com.kuber.server.factory.CacheConfig;
import com.kuber.server.factory.CacheProxy;
import com.kuber.server.factory.FactoryProvider;
import com.kuber.server.persistence.PersistenceOperationLock;
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.replication.ReplicationOpLog;
import com.kuber.server.replication.ReplicationOpLogEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * @version 2.6.0
 */
@Slf4j
@Service
public class CacheService {
    
    private final KuberProperties properties;
    private final PersistenceStore persistenceStore;
    private final EventPublisher eventPublisher;
    private final CacheMetricsService metricsService;
    private final PersistenceOperationLock operationLock;
    private final FactoryProvider factoryProvider;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    @Autowired(required = false)
    private ReplicationOpLog replicationOpLog;
    
    // BackupRestoreService for region lock checking during restore
    private com.kuber.server.backup.BackupRestoreService backupRestoreService;
    
    // ==================== HYBRID MEMORY ARCHITECTURE ====================
    // Key Index: ALL keys always in memory for O(1) lookups (per region)
    // Key Index: ALL keys always in memory (per region) - can be on-heap or off-heap
    private final Map<String, KeyIndexInterface> keyIndices = new ConcurrentHashMap<>();
    
    // Value Cache: Hot values in memory (can be evicted to disk)
    // Uses CacheProxy abstraction for pluggable cache implementations (v1.5.0)
    private final Map<String, CacheProxy<String, CacheEntry>> regionCaches = new ConcurrentHashMap<>();
    
    // Region metadata
    private final Map<String, CacheRegion> regions = new ConcurrentHashMap<>();
    
    // Effective memory limits per region (for value cache only - keys are always in memory)
    private final Map<String, Integer> effectiveMemoryLimits = new ConcurrentHashMap<>();
    
    // Statistics
    private final Map<String, Map<String, Long>> statistics = new ConcurrentHashMap<>();
    
    // v2.2.0: Eviction counters per region (for periodic summary logging)
    private final Map<String, AtomicLong> evictionCounters = new ConcurrentHashMap<>();
    
    // v2.6.0: Track keys whose expired events were already published by the Caffeine listener
    // Prevents double-publishing when cleanupExpiredEntries() runs after Caffeine eviction
    private final Set<String> recentlyPublishedExpired = ConcurrentHashMap.newKeySet();
    
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
                        PersistenceOperationLock operationLock,
                        FactoryProvider factoryProvider) {
        this.properties = properties;
        this.persistenceStore = persistenceStore;
        this.eventPublisher = eventPublisher;
        this.metricsService = metricsService;
        this.operationLock = operationLock;
        this.factoryProvider = factoryProvider;
        
        log.info("CacheService initialized with cache implementation: {}, map implementation: {}",
                factoryProvider.getCacheFactory().getType(),
                factoryProvider.getCollectionsFactory().getType());
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
        log.info("Initializing Kuber cache service v{} with HYBRID MEMORY ARCHITECTURE...", properties.getVersion());
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
            // Register region for metrics tracking
            metricsService.registerRegion(regionName);
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
    
    /**
     * Check if shutdown is in progress.
     * Used by AutoloadService and warming to stop processing during shutdown.
     * 
     * @return true if shutdown has been initiated
     * @since 1.6.3
     */
    public boolean isShuttingDown() {
        return shuttingDown;
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
        
        // Create value cache using factory (v1.5.0 - pluggable cache implementation)
        // Configure cache with eviction listener to update KeyIndex when values are evicted
        CacheConfig cacheConfig = CacheConfig.builder()
                .maximumSize(memoryLimit)
                .expireAfterWrite(Duration.ofHours(24))
                .synchronousExecution(true)  // Avoid ForkJoinPool issues during shutdown
                .recordStats(true)
                .removalListenerWithCause((key, value, cause) -> {
                    // Skip removal listener work during shutdown
                    if (shuttingDown) {
                        return;
                    }
                    
                    // When value is evicted from memory, update KeyIndex to mark as DISK only
                    if (cause == CacheConfig.RemovalCause.SIZE || cause == CacheConfig.RemovalCause.EXPIRED) {
                        KeyIndexInterface idx = keyIndices.get(regionName);
                        if (idx != null && key != null) {
                            if (cause == CacheConfig.RemovalCause.SIZE) {
                                // Value evicted due to size - still exists on disk
                                idx.updateLocation((String) key, ValueLocation.DISK);
                                // v2.2.0: Increment eviction counter for periodic summary logging
                                evictionCounters.computeIfAbsent(regionName, k -> new AtomicLong(0)).incrementAndGet();
                            } else if (cause == CacheConfig.RemovalCause.EXPIRED) {
                                // v2.6.0: Publish expired event immediately with payload
                                // The value is available here before Caffeine discards it.
                                // cleanupExpiredEntries() handles keyIndex/persistence cleanup;
                                // we publish here for immediate notification with the full value.
                                if (value instanceof CacheEntry cacheEntry) {
                                    String entryValue = cacheEntry.getStringValue();
                                    String compositeKey = regionName + "|" + (String) key;
                                    recentlyPublishedExpired.add(compositeKey);
                                    eventPublisher.publish(CacheEvent.entryExpired(regionName, (String) key, properties.getNodeId()));
                                    if (publishingService != null) {
                                        publishingService.publishExpire(regionName, (String) key, entryValue, properties.getNodeId());
                                    }
                                }
                                recordStatistic(regionName, KuberConstants.STAT_EXPIRED);
                            }
                        }
                        recordStatistic(regionName, KuberConstants.STAT_EVICTED);
                    }
                })
                .build();
        
        CacheProxy<String, CacheEntry> cache = factoryProvider.getCacheFactory()
                .createCache("region-" + regionName, cacheConfig);
        
        regionCaches.put(regionName, cache);
        statistics.put(regionName, new ConcurrentHashMap<>());
        
        log.debug("Created {} key index and {} value cache for region '{}' (value cache limit: {})", 
                keyIndex.isOffHeap() ? "OFF-HEAP" : "ON-HEAP", 
                cache.getType(),
                regionName, memoryLimit);
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
        log.info("  - Hot values will be loaded into value cache (up to warm-percentage)");
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
     * 
     * <p>STARTUP OPTIMIZATION (v1.6.3):
     * <ul>
     *   <li>Uses STREAMING to avoid loading all entries into memory</li>
     *   <li>ALL keys are loaded into KeyIndex (for fast EXISTS/KEYS operations)</li>
     *   <li>Only warmPercentage of values are loaded into cache (reduces heap pressure)</li>
     *   <li>Remaining values loaded on-demand via GET (lazy loading)</li>
     * </ul>
     * 
     * @return array of [keysLoaded, valuesLoaded]
     */
    private long[] primeRegion(String regionName) {
        // Mark region as loading - queries should wait
        operationLock.markRegionLoading(regionName);
        
        try {
            KeyIndexInterface keyIndex = keyIndices.get(regionName);
            CacheProxy<String, CacheEntry> valueCache = regionCaches.get(regionName);
            int valueMemoryLimit = getEffectiveMemoryLimit(regionName);
            
            // Get warm percentage from config (default: 10%)
            int warmPercentage = properties.getAutoload().getWarmPercentage();
            int warmLimit = (warmPercentage > 0) ? (int) ((valueMemoryLimit * warmPercentage) / 100.0) : 0;
            
            if (keyIndex == null || valueCache == null) {
                log.warn("KeyIndex or value cache not found for region '{}', skipping priming", regionName);
                return new long[]{0, 0};
            }
            
            // Use atomic counters for streaming
            final java.util.concurrent.atomic.AtomicLong keysLoaded = new java.util.concurrent.atomic.AtomicLong(0);
            final java.util.concurrent.atomic.AtomicLong valuesLoaded = new java.util.concurrent.atomic.AtomicLong(0);
            final java.util.concurrent.atomic.AtomicLong lastLoggedCount = new java.util.concurrent.atomic.AtomicLong(0);
            final int finalWarmLimit = warmLimit;
            final long startTime = System.currentTimeMillis();
            
            // Log estimated count before streaming
            long estimatedCount = persistenceStore.estimateEntryCount(regionName);
            log.info("Region '{}': starting to stream ~{} entries (warm-limit for values: {})", 
                    regionName, estimatedCount, warmLimit);
            
            // Progress logging interval - every 10,000 entries
            final long PROGRESS_INTERVAL = 10000;
            
            // STREAMING approach (v1.6.3): iterate without loading all into memory
            // ALL keys go to KeyIndex, only warmLimit values go to cache
            persistenceStore.forEachEntry(regionName, entry -> {
                if (!entry.isExpired()) {
                    // Always add key to index (ALL keys in memory - core design)
                    long currentValuesLoaded = valuesLoaded.get();
                    boolean valueInMemory = (finalWarmLimit > 0) && (currentValuesLoaded < finalWarmLimit);
                    ValueLocation location = valueInMemory ? ValueLocation.BOTH : ValueLocation.DISK;
                    keyIndex.putFromCacheEntry(entry, location);
                    long currentKeys = keysLoaded.incrementAndGet();
                    
                    // Only add hot values to cache (up to warm limit)
                    if (valueInMemory) {
                        valueCache.put(entry.getKey(), entry);
                        valuesLoaded.incrementAndGet();
                    }
                    
                    // Log progress every 10,000 entries
                    long lastLogged = lastLoggedCount.get();
                    if (currentKeys - lastLogged >= PROGRESS_INTERVAL) {
                        if (lastLoggedCount.compareAndSet(lastLogged, currentKeys)) {
                            long elapsed = System.currentTimeMillis() - startTime;
                            log.info("Region '{}': PROGRESS - {} keys loaded, {} values warmed ({} ms elapsed)", 
                                    regionName, currentKeys, valuesLoaded.get(), elapsed);
                        }
                    }
                }
            });
            
            long finalKeysLoaded = keysLoaded.get();
            long finalValuesLoaded = valuesLoaded.get();
            long totalElapsed = System.currentTimeMillis() - startTime;
            
            // Final completion log
            if (warmPercentage > 0) {
                log.info("Region '{}': COMPLETED - {} keys into index, {} values into cache (warm {}% of limit {}) in {} ms", 
                        regionName, finalKeysLoaded, finalValuesLoaded, warmPercentage, valueMemoryLimit, totalElapsed);
            } else {
                log.info("Region '{}': COMPLETED - {} keys into index, 0 values (warming disabled) in {} ms", 
                        regionName, finalKeysLoaded, totalElapsed);
            }
            
            return new long[]{finalKeysLoaded, finalValuesLoaded};
            
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
        
        // Register region for metrics tracking
        metricsService.registerRegion(name);
        
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
        
        // Append to replication oplog (PRIMARY only)
        if (replicationOpLog != null && (replicationManager == null || replicationManager.isPrimary())) {
            replicationOpLog.append(ReplicationOpLogEntry.regionCreate(region));
        }
        
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
        waitForRegionLoadingIfNeeded(name, "deleteRegion");
        
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
        CacheProxy<String, CacheEntry> cache = regionCaches.remove(name);
        if (cache != null) {
            cache.invalidateAll();
        }
        
        regions.remove(name);
        statistics.remove(name);
        
        // Delete from persistence store
        persistenceStore.deleteRegion(name);
        
        eventPublisher.publish(CacheEvent.regionDeleted(name, properties.getNodeId()));
        
        // Append to replication oplog (PRIMARY only)
        if (replicationOpLog != null && (replicationManager == null || replicationManager.isPrimary())) {
            replicationOpLog.append(ReplicationOpLogEntry.regionDelete(name));
        }
        
        log.info("Deleted region: {} (KeyIndex and value cache cleared)", name);
    }
    
    public void purgeRegion(String name) {
        checkWriteAccess();
        waitForRegionLoadingIfNeeded(name, "purgeRegion");
        
        CacheRegion region = regions.get(name);
        if (region == null) {
            throw RegionException.notFound(name);
        }
        
        // Collect keys BEFORE purging for broker event publishing
        KeyIndexInterface keyIndex = keyIndices.get(name);
        Set<String> keysBeforePurge = null;
        if (publishingService != null && keyIndex != null) {
            keysBeforePurge = keyIndex.getAllKeys();
        }
        
        // Clear KeyIndex for this region
        if (keyIndex != null) {
            keyIndex.clear();
        }
        
        // Clear value cache
        CacheProxy<String, CacheEntry> cache = regionCaches.get(name);
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
        
        // Publish DELETE events to brokers for each key that was purged
        if (publishingService != null && keysBeforePurge != null && !keysBeforePurge.isEmpty()) {
            for (String key : keysBeforePurge) {
                publishingService.publishDelete(name, key, properties.getNodeId());
            }
            log.info("Published {} DELETE events for purged region '{}'", keysBeforePurge.size(), name);
        }
        
        // Append to replication oplog (PRIMARY only)
        if (replicationOpLog != null && (replicationManager == null || replicationManager.isPrimary())) {
            replicationOpLog.append(ReplicationOpLogEntry.regionPurge(name));
        }
        
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
        log.debug("getAllRegions returning {} regions: {}", regions.size(), regions.keySet());
        return Collections.unmodifiableCollection(regions.values());
    }
    
    public CacheRegion getRegion(String name) {
        CacheRegion region = regions.get(name);
        if (region != null) {
            // Wait for autoload to complete before collecting stats
            waitForRegionLoadingIfNeeded(name, "getRegion stats");
            
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
        
        // Validate key length before any processing
        validateKeyLength(region, key, value);
        
        waitForRegionLoadingIfNeeded(region, "SET");
        
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
        
        // Validate key length before any processing
        validateKeyLength(region, key, value);
        
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
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
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
            CacheEntry entry = valueCache.get(key);
            
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
        waitForRegionLoadingIfNeeded(region, "DELETE");
        
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
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
            
            // Append to replication oplog (PRIMARY only)
            if (replicationOpLog != null && (replicationManager == null || replicationManager.isPrimary())) {
                replicationOpLog.append(ReplicationOpLogEntry.delete(region, key));
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
        ensureRegionExists(region);
        waitForRegionLoadingIfNeeded(region, "EXPIRE");
        
        CacheEntry entry = getEntry(region, key);
        if (entry == null) {
            return false;
        }
        
        entry.setTtlSeconds(ttlSeconds);
        entry.setExpiresAt(Instant.now().plusSeconds(ttlSeconds));
        entry.setUpdatedAt(Instant.now());
        
        putEntry(region, key, entry);
        
        String value = entry.getStringValue();
        eventPublisher.publish(CacheEvent.entrySet(region, key, value, properties.getNodeId()));
        
        // Trigger async event publishing (Kafka/ActiveMQ/RabbitMQ/IBM MQ/File)
        if (publishingService != null) {
            publishingService.publishUpdate(region, key, value, properties.getNodeId());
        }
        
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
        ensureRegionExists(region);
        waitForRegionLoadingIfNeeded(region, "PERSIST");
        
        CacheEntry entry = getEntry(region, key);
        if (entry == null) {
            return false;
        }
        
        entry.setTtlSeconds(-1);
        entry.setExpiresAt(null);
        entry.setUpdatedAt(Instant.now());
        
        putEntry(region, key, entry);
        
        String value = entry.getStringValue();
        eventPublisher.publish(CacheEvent.entrySet(region, key, value, properties.getNodeId()));
        
        // Trigger async event publishing (Kafka/ActiveMQ/RabbitMQ/IBM MQ/File)
        if (publishingService != null) {
            publishingService.publishUpdate(region, key, value, properties.getNodeId());
        }
        
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
     * Get keys matching a regex pattern (keys only, no values).
     * This is more efficient than searchKeysByRegex() when you only need key names.
     * Uses KeyIndex for fast key lookup (always in memory - no disk I/O).
     * 
     * @param region the cache region
     * @param regexPattern a Java regex pattern to match keys (e.g., "^user:\\d+$")
     * @return list of matching keys
     */
    public List<String> keysByRegex(String region, String regexPattern) {
        return keysByRegex(region, regexPattern, 1000);
    }
    
    /**
     * Get keys matching a regex pattern with limit (keys only, no values).
     * This is more efficient than searchKeysByRegex() when you only need key names.
     * Uses KeyIndex for fast key lookup (always in memory - no disk I/O).
     * 
     * @param region the cache region
     * @param regexPattern a Java regex pattern to match keys (e.g., "^user:\\d+$")
     * @param limit maximum number of keys to return
     * @return list of matching keys
     */
    public List<String> keysByRegex(String region, String regexPattern, int limit) {
        ensureRegionExists(region);
        
        Pattern pattern;
        try {
            pattern = Pattern.compile(regexPattern);
        } catch (Exception e) {
            throw new KuberException(KuberException.ErrorCode.INVALID_ARGUMENT, 
                    "Invalid regex pattern: " + e.getMessage());
        }
        
        List<String> matchingKeys = new ArrayList<>();
        
        // Use KeyIndex for fast key iteration (always in memory - no disk I/O!)
        KeyIndexInterface keyIndex = keyIndices.get(region);
        
        for (String key : keyIndex.getAllKeys()) {
            if (pattern.matcher(key).matches()) {
                matchingKeys.add(key);
                if (matchingKeys.size() >= limit) {
                    break;
                }
            }
        }
        
        return matchingKeys;
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
     * Uses KeyIndex for fast key lookup (always in memory), then batch loads values.
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
        
        // Use KeyIndex for fast key iteration (always in memory!)
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> cache = regionCaches.get(region);
        
        // First pass: find matching keys from KeyIndex
        List<String> matchingKeys = new ArrayList<>();
        for (String key : keyIndex.getAllKeys()) {
            if (pattern.matcher(key).matches()) {
                matchingKeys.add(key);
                if (matchingKeys.size() >= limit) {
                    break;
                }
            }
        }
        
        if (matchingKeys.isEmpty()) {
            return results;
        }
        
        // Separate keys into those in cache vs those needing persistence lookup
        List<String> keysToLoadFromPersistence = new ArrayList<>();
        
        for (String key : matchingKeys) {
            CacheEntry entry = cache.get(key);
            if (entry != null && !entry.isExpired()) {
                results.add(entryToResultMap(entry));
                recordStatistic(region, KuberConstants.STAT_HITS);
            } else {
                keysToLoadFromPersistence.add(key);
            }
        }
        
        // Batch load remaining keys from persistence
        if (!keysToLoadFromPersistence.isEmpty()) {
            Map<String, CacheEntry> loaded = persistenceStore.loadEntriesByKeys(region, keysToLoadFromPersistence);
            for (String key : keysToLoadFromPersistence) {
                CacheEntry entry = loaded.get(key);
                if (entry != null && !entry.isExpired()) {
                    // Put in cache for future access
                    cache.put(key, entry);
                    keyIndex.updateLocation(key, ValueLocation.BOTH);
                    results.add(entryToResultMap(entry));
                    recordStatistic(region, KuberConstants.STAT_HITS);
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
        
        delete(region, oldKey); // Fires DELETE event for oldKey
        putEntry(region, newKey, entry);
        
        // Fire INSERT event for the new key
        String value = entry.getStringValue();
        eventPublisher.publish(CacheEvent.entrySet(region, newKey, value, properties.getNodeId()));
        
        if (publishingService != null) {
            publishingService.publishInsert(region, newKey, value, properties.getNodeId());
        }
        
        return true;
    }
    
    // ==================== JSON Operations ====================
    
    public void jsonSet(String region, String key, JsonNode value) {
        jsonSet(region, key, "$", value, -1);
    }
    
    public void jsonSet(String region, String key, String path, JsonNode value, long ttlSeconds) {
        checkWriteAccess();
        ensureRegionExists(region);
        waitForRegionLoadingIfNeeded(region, "JSONSET");
        
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
        
        String jsonString = JsonUtils.toJson(finalValue);
        eventPublisher.publish(CacheEvent.entrySet(region, key, jsonString, properties.getNodeId()));
        
        // Trigger async event publishing (Kafka/ActiveMQ/RabbitMQ/IBM MQ/File)
        if (publishingService != null) {
            if (existing != null) {
                publishingService.publishUpdate(region, key, jsonString, properties.getNodeId());
            } else {
                publishingService.publishInsert(region, key, jsonString, properties.getNodeId());
            }
        }
    }
    
    /**
     * Update/merge JSON value (upsert with merge).
     * 
     * Behavior:
     * - If key doesn't exist: set the JSON value
     * - If key exists and is valid JSON: merge new JSON onto existing (deep merge, new values override)
     * - If key exists but is not valid JSON: replace with new JSON value
     * 
     * @param region the region name
     * @param key the cache key
     * @param newJson the JSON value to update/merge
     * @param ttlSeconds TTL in seconds (-1 for no expiry)
     * @return the resulting merged JSON value
     */
    public JsonNode jsonUpdate(String region, String key, JsonNode newJson, long ttlSeconds) {
        checkWriteAccess();
        ensureRegionExists(region);
        waitForRegionLoadingIfNeeded(region, "JUPDATE");
        
        Instant now = Instant.now();
        Instant expiresAt = ttlSeconds > 0 ? now.plusSeconds(ttlSeconds) : null;
        
        CacheEntry existing = getEntry(region, key);
        JsonNode finalValue;
        
        if (existing == null) {
            // Key doesn't exist: set the new JSON value
            finalValue = applyAttributeMapping(region, newJson);
            log.debug("JUPDATE: key '{}' not found, setting new value", key);
        } else if (existing.getJsonValue() != null && existing.getJsonValue().isObject() && newJson.isObject()) {
            // Key exists with valid JSON object: merge
            finalValue = mergeJsonNodes(existing.getJsonValue(), newJson);
            log.debug("JUPDATE: merging JSON for key '{}'", key);
        } else if (existing.getValueType() == CacheEntry.ValueType.JSON && existing.getJsonValue() != null) {
            // Key exists with JSON but can't merge (e.g., arrays): replace
            finalValue = applyAttributeMapping(region, newJson);
            log.debug("JUPDATE: replacing non-object JSON for key '{}'", key);
        } else {
            // Key exists but not valid JSON: replace with new JSON
            finalValue = applyAttributeMapping(region, newJson);
            log.debug("JUPDATE: replacing non-JSON value for key '{}'", key);
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
        
        String jsonString = JsonUtils.toJson(finalValue);
        eventPublisher.publish(CacheEvent.entrySet(region, key, jsonString, properties.getNodeId()));
        
        // Trigger async event publishing (Kafka/ActiveMQ/RabbitMQ/IBM MQ/File)
        if (publishingService != null) {
            if (existing != null) {
                publishingService.publishUpdate(region, key, jsonString, properties.getNodeId());
            } else {
                publishingService.publishInsert(region, key, jsonString, properties.getNodeId());
            }
        }
        
        return finalValue;
    }
    
    /**
     * Deep merge two JSON nodes. Values from newJson override values from existingJson.
     * For nested objects, merge recursively. Arrays are replaced, not merged.
     */
    private JsonNode mergeJsonNodes(JsonNode existingJson, JsonNode newJson) {
        if (existingJson == null || !existingJson.isObject()) {
            return newJson;
        }
        if (newJson == null || !newJson.isObject()) {
            return existingJson;
        }
        
        // Convert to Map for merging
        Map<String, Object> existingMap = JsonUtils.getObjectMapper()
                .convertValue(existingJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        Map<String, Object> newMap = JsonUtils.getObjectMapper()
                .convertValue(newJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        
        // Deep merge
        deepMerge(existingMap, newMap);
        
        return JsonUtils.getObjectMapper().valueToTree(existingMap);
    }
    
    /**
     * Deep merge newMap into existingMap. Modifies existingMap in place.
     */
    @SuppressWarnings("unchecked")
    private void deepMerge(Map<String, Object> existingMap, Map<String, Object> newMap) {
        for (Map.Entry<String, Object> entry : newMap.entrySet()) {
            String key = entry.getKey();
            Object newValue = entry.getValue();
            
            if (existingMap.containsKey(key)) {
                Object existingValue = existingMap.get(key);
                
                // If both are maps, merge recursively
                if (existingValue instanceof Map && newValue instanceof Map) {
                    deepMerge((Map<String, Object>) existingValue, (Map<String, Object>) newValue);
                } else {
                    // Otherwise, new value overrides
                    existingMap.put(key, newValue);
                }
            } else {
                // Key doesn't exist in existing map, add it
                existingMap.put(key, newValue);
            }
        }
    }
    
    /**
     * Remove specified attributes from a JSON value.
     * 
     * Behavior:
     * - If key exists and has valid JSON object: removes specified attributes and saves
     * - If key doesn't exist or value is not JSON object: returns null (nothing done)
     * 
     * @param region the region name
     * @param key the cache key
     * @param attributes list of attribute names to remove
     * @return the updated JSON value, or null if nothing was done
     */
    public JsonNode jsonRemoveAttributes(String region, String key, List<String> attributes) {
        checkWriteAccess();
        ensureRegionExists(region);
        waitForRegionLoadingIfNeeded(region, "JREMOVE");
        
        CacheEntry existing = getEntry(region, key);
        
        // If key doesn't exist, return null
        if (existing == null) {
            log.debug("JREMOVE: key '{}' not found, nothing to do", key);
            return null;
        }
        
        // If value is not a JSON object, return null
        if (existing.getJsonValue() == null || !existing.getJsonValue().isObject()) {
            log.debug("JREMOVE: value for key '{}' is not a JSON object, nothing to do", key);
            return null;
        }
        
        // Convert to Map for attribute removal
        Map<String, Object> jsonMap = JsonUtils.getObjectMapper()
                .convertValue(existing.getJsonValue(), new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        
        // Remove specified attributes
        int removedCount = 0;
        for (String attr : attributes) {
            if (jsonMap.containsKey(attr)) {
                jsonMap.remove(attr);
                removedCount++;
                log.debug("JREMOVE: removed attribute '{}' from key '{}'", attr, key);
            }
        }
        
        // If nothing was removed, return the original JSON (no changes needed)
        if (removedCount == 0) {
            log.debug("JREMOVE: no attributes found to remove from key '{}'", key);
            return existing.getJsonValue();
        }
        
        // Convert back to JsonNode and save
        JsonNode updatedJson = JsonUtils.getObjectMapper().valueToTree(jsonMap);
        
        existing.setJsonValue(updatedJson);
        existing.setStringValue(JsonUtils.toJson(updatedJson));
        existing.setUpdatedAt(Instant.now());
        
        putEntry(region, key, existing);
        
        String jsonString = JsonUtils.toJson(updatedJson);
        eventPublisher.publish(CacheEvent.entrySet(region, key, jsonString, properties.getNodeId()));
        
        // Trigger async event publishing (Kafka/ActiveMQ/RabbitMQ/IBM MQ/File)
        if (publishingService != null) {
            publishingService.publishUpdate(region, key, jsonString, properties.getNodeId());
        }
        
        log.debug("JREMOVE: removed {} attribute(s) from key '{}'", removedCount, key);
        return updatedJson;
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
    
    /**
     * Batch get JSON documents for multiple keys.
     * v2.2.0: Optimized for index rebuild - reduces GC pressure by batching.
     * 
     * @param region Region name
     * @param keys List of keys to retrieve
     * @return Map of key to JsonNode (only non-null values included)
     */
    public Map<String, JsonNode> jsonGetBatch(String region, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        
        Map<String, JsonNode> result = new HashMap<>(keys.size());
        CacheProxy<String, CacheEntry> cache = regionCaches.get(region);
        
        if (cache == null) {
            return result;
        }
        
        // Batch get from cache using asMap() for efficiency
        Map<String, CacheEntry> cacheMap = cache.asMap();
        
        for (String key : keys) {
            CacheEntry cacheEntry = cacheMap.get(key);
            if (cacheEntry != null && cacheEntry.getJsonValue() != null && !cacheEntry.isExpired()) {
                result.put(key, cacheEntry.getJsonValue());
            }
        }
        
        return result;
    }
    
    /**
     * Stream entries directly from persistence store, bypassing cache.
     * v2.2.0: Used for index rebuild to avoid cache eviction pressure.
     * 
     * <p>This method reads directly from disk without loading into cache,
     * which prevents eviction cascades during large index rebuilds.
     * 
     * @param region Region name
     * @param consumer Consumer to process each entry (key, JsonNode)
     * @return Number of entries processed
     */
    public long streamEntriesFromPersistence(String region, java.util.function.BiConsumer<String, JsonNode> consumer) {
        if (persistenceStore == null) {
            log.warn("No persistence store available for streaming");
            return 0;
        }
        
        AtomicLong count = new AtomicLong(0);
        
        persistenceStore.forEachEntry(region, entry -> {
            if (entry != null && !entry.isExpired() && entry.getJsonValue() != null) {
                consumer.accept(entry.getKey(), entry.getJsonValue());
                count.incrementAndGet();
            }
        });
        
        return count.get();
    }
    
    /**
     * Check if persistence store is available.
     * @return true if persistence is configured and available
     */
    public boolean hasPersistenceStore() {
        return persistenceStore != null && persistenceStore.getType() != PersistenceStore.PersistenceType.MEMORY;
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
        
        String jsonString = JsonUtils.toJson(updated);
        eventPublisher.publish(CacheEvent.entrySet(region, key, jsonString, properties.getNodeId()));
        
        // Trigger async event publishing (Kafka/ActiveMQ/RabbitMQ/IBM MQ/File)
        if (publishingService != null) {
            publishingService.publishUpdate(region, key, jsonString, properties.getNodeId());
        }
        
        return true;
    }
    
    public List<CacheEntry> jsonSearch(String region, String query) {
        return jsonSearch(region, query, 1000);
    }
    
    /**
     * Search JSON entries by query with limit.
     * Uses KeyIndex for fast key lookup (always in memory), then batch loads values.
     */
    public List<CacheEntry> jsonSearch(String region, String query, int limit) {
        ensureRegionExists(region);
        
        List<JsonUtils.QueryCondition> conditions = JsonUtils.parseQuery(query);
        List<CacheEntry> results = new ArrayList<>();
        
        // Use KeyIndex for key iteration (always in memory!)
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> cache = regionCaches.get(region);
        
        // Get all keys from KeyIndex (limited scan)
        List<String> allKeys = new ArrayList<>();
        int maxKeysToScan = limit * 10; // Scan up to 10x limit to find enough matches
        int scanned = 0;
        
        for (String key : keyIndex.getAllKeys()) {
            allKeys.add(key);
            scanned++;
            if (scanned >= maxKeysToScan) {
                break;
            }
        }
        
        if (allKeys.isEmpty()) {
            return results;
        }
        
        // Separate keys into those in cache vs those needing persistence lookup
        List<String> keysToLoadFromPersistence = new ArrayList<>();
        Set<String> processedKeys = new HashSet<>();
        
        // First check in-memory cache
        for (String key : allKeys) {
            if (results.size() >= limit) {
                break;
            }
            
            CacheEntry entry = cache.get(key);
            if (entry != null) {
                processedKeys.add(key);
                if (entry.getValueType() == CacheEntry.ValueType.JSON 
                        && entry.getJsonValue() != null 
                        && !entry.isExpired()
                        && JsonUtils.matchesAllQueries(entry.getJsonValue(), conditions)) {
                    results.add(entry);
                }
            } else {
                // Need to load from persistence
                keysToLoadFromPersistence.add(key);
            }
        }
        
        // Batch load remaining keys from persistence if we need more results
        if (results.size() < limit && !keysToLoadFromPersistence.isEmpty()) {
            // Load in batches to avoid memory issues
            final int BATCH_SIZE = 1000;
            
            for (int i = 0; i < keysToLoadFromPersistence.size() && results.size() < limit; i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, keysToLoadFromPersistence.size());
                List<String> batch = keysToLoadFromPersistence.subList(i, end);
                
                Map<String, CacheEntry> loaded = persistenceStore.loadEntriesByKeys(region, batch);
                
                for (String key : batch) {
                    if (results.size() >= limit) {
                        break;
                    }
                    
                    CacheEntry entry = loaded.get(key);
                    if (entry != null) {
                        // Put in cache for future access
                        cache.put(key, entry);
                        keyIndex.updateLocation(key, ValueLocation.BOTH);
                        
                        if (entry.getValueType() == CacheEntry.ValueType.JSON 
                                && entry.getJsonValue() != null 
                                && !entry.isExpired()
                                && JsonUtils.matchesAllQueries(entry.getJsonValue(), conditions)) {
                            results.add(entry);
                        }
                    }
                }
            }
        }
        
        return results;
    }
    
    // ==================== Hash Operations ====================
    
    public void hset(String region, String key, String field, String value) {
        checkWriteAccess();
        ensureRegionExists(region);
        
        // Validate key length before any processing
        validateKeyLength(region, key, value);
        
        waitForRegionLoadingIfNeeded(region, "HSET");
        
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
     * Validate that a key does not exceed the maximum allowed length.
     * 
     * @param region the region name
     * @param key the key to validate
     * @param value the value (for logging purposes if validation fails)
     * @throws KuberException if key length exceeds the configured maximum
     */
    private void validateKeyLength(String region, String key, String value) {
        int maxKeyLengthBytes = properties.getCache().getMaxKeyLengthBytes();
        int keyLengthBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        
        if (keyLengthBytes > maxKeyLengthBytes) {
            // Log the rejected key and value for debugging
            String truncatedValue = value != null && value.length() > 500 
                    ? value.substring(0, 500) + "... [TRUNCATED]" 
                    : value;
            
            log.error("╔════════════════════════════════════════════════════════════════════════════╗");
            log.error("║  KEY LENGTH EXCEEDED - ENTRY REJECTED                                       ║");
            log.error("╠════════════════════════════════════════════════════════════════════════════╣");
            log.error("║  Region: {}", region);
            log.error("║  Key Length: {} bytes (max allowed: {} bytes)", keyLengthBytes, maxKeyLengthBytes);
            log.error("║  Key: {}", key);
            log.error("║  Value: {}", truncatedValue);
            log.error("╚════════════════════════════════════════════════════════════════════════════╝");
            
            throw new KuberException(String.format(
                    "Key length %d bytes exceeds maximum allowed %d bytes for region '%s'. Key: %s",
                    keyLengthBytes, maxKeyLengthBytes, region, 
                    key.length() > 100 ? key.substring(0, 100) + "..." : key));
        }
    }
    
    /**
     * Validate key length for CacheEntry.
     * Returns true if valid, false if invalid (and removes from heap if present).
     * 
     * @param entry the cache entry to validate
     * @return true if key length is valid, false otherwise
     */
    private boolean validateKeyLengthForEntry(CacheEntry entry) {
        int maxKeyLengthBytes = properties.getCache().getMaxKeyLengthBytes();
        String key = entry.getKey();
        int keyLengthBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        
        if (keyLengthBytes > maxKeyLengthBytes) {
            String region = entry.getRegion();
            String value = entry.getStringValue();
            String truncatedValue = value != null && value.length() > 500 
                    ? value.substring(0, 500) + "... [TRUNCATED]" 
                    : value;
            
            log.error("╔════════════════════════════════════════════════════════════════════════════╗");
            log.error("║  KEY LENGTH EXCEEDED - ENTRY REJECTED                                       ║");
            log.error("╠════════════════════════════════════════════════════════════════════════════╣");
            log.error("║  Region: {}", region);
            log.error("║  Key Length: {} bytes (max allowed: {} bytes)", keyLengthBytes, maxKeyLengthBytes);
            log.error("║  Key: {}", key);
            log.error("║  Value: {}", truncatedValue);
            log.error("╚════════════════════════════════════════════════════════════════════════════╝");
            
            // Remove from heap if already added
            removeFromHeapIfPresent(region, key);
            
            return false;
        }
        return true;
    }
    
    /**
     * Remove entry from heap (KeyIndex and ValueCache) if present.
     * Used when a key is rejected due to length validation.
     */
    private void removeFromHeapIfPresent(String region, String key) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex != null) {
            KeyIndexEntry removed = keyIndex.remove(key);
            if (removed != null) {
                log.debug("Removed oversized key from KeyIndex: region='{}', key='{}'", region, key);
            }
        }
        
        if (valueCache != null) {
            valueCache.invalidate(key);
            log.debug("Invalidated oversized key from ValueCache: region='{}', key='{}'", region, key);
        }
    }
    
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
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
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
        CacheEntry entry = valueCache.get(key);
        
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
    private void handleExpiredEntry(String region, String key, KeyIndexInterface keyIndex, CacheProxy<String, CacheEntry> valueCache) {
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
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
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
        
        // Append to replication oplog (PRIMARY only)
        if (replicationOpLog != null && (replicationManager == null || replicationManager.isPrimary())) {
            replicationOpLog.append(ReplicationOpLogEntry.set(region, key, entry));
        }
    }
    
    /**
     * Batch put entries for better performance during bulk operations like autoload.
     * Entries are grouped by region and saved in batches to the persistence store.
     * 
     * @param entries list of cache entries to save
     * @return number of entries successfully saved
     */
    /**
     * Batch put entries for better performance during bulk operations.
     * Populates value cache (may trigger eviction).
     * 
     * @param entries list of cache entries to save
     * @return number of entries successfully saved
     */
    public int putEntriesBatch(List<CacheEntry> entries) {
        return putEntriesBatch(entries, false);
    }
    
    /**
     * Batch put entries for better performance during bulk operations like autoload.
     * Entries are grouped by region and saved in batches to the persistence store.
     * 
     * <p><b>Autoload Mode (skipValueCache=true):</b>
     * <ul>
     *   <li>Only updates KeyIndex and persistence store</li>
     *   <li>Skips value cache population entirely</li>
     *   <li>No eviction pressure - memory stays within bounds</li>
     *   <li>Data is loaded on-demand when accessed (lazy loading)</li>
     *   <li>KeyIndex marks entries as PERSISTENCE_ONLY</li>
     * </ul>
     * 
     * <p><b>Normal Mode (skipValueCache=false):</b>
     * <ul>
     *   <li>Updates KeyIndex, persistence store, AND value cache</li>
     *   <li>May trigger eviction if cache is full</li>
     *   <li>Data immediately available in memory</li>
     * </ul>
     * 
     * @param entries list of cache entries to save
     * @param skipValueCache if true, skip value cache population (for autoload)
     * @return number of entries successfully saved
     */
    public int putEntriesBatch(List<CacheEntry> entries, boolean skipValueCache) {
        if (entries == null || entries.isEmpty()) {
            log.debug("putEntriesBatch called with null or empty list");
            return 0;
        }
        
        log.debug("putEntriesBatch called with {} entries (skipValueCache={})", entries.size(), skipValueCache);
        
        checkWriteAccess();
        
        // Validate key lengths and filter out oversized keys
        List<CacheEntry> validEntries = new ArrayList<>();
        int rejectedCount = 0;
        for (CacheEntry entry : entries) {
            if (validateKeyLengthForEntry(entry)) {
                validEntries.add(entry);
            } else {
                rejectedCount++;
            }
        }
        
        if (rejectedCount > 0) {
            log.warn("Rejected {} entries due to key length exceeding {} bytes", 
                    rejectedCount, properties.getCache().getMaxKeyLengthBytes());
        }
        
        if (validEntries.isEmpty()) {
            log.warn("All entries rejected due to key length validation");
            return 0;
        }
        
        // Group entries by region
        Map<String, List<CacheEntry>> entriesByRegion = new LinkedHashMap<>();
        for (CacheEntry entry : validEntries) {
            entriesByRegion.computeIfAbsent(entry.getRegion(), k -> new ArrayList<>()).add(entry);
        }
        
        int savedCount = 0;
        
        for (Map.Entry<String, List<CacheEntry>> regionGroup : entriesByRegion.entrySet()) {
            String region = regionGroup.getKey();
            List<CacheEntry> regionEntries = regionGroup.getValue();
            
            log.debug("Processing batch of {} entries for region '{}'", regionEntries.size(), region);
            
            // Ensure region exists
            ensureRegionExists(region);
            
            KeyIndexInterface keyIndex = keyIndices.get(region);
            CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
            
            if (keyIndex == null) {
                log.warn("Region '{}' KeyIndex not initialized, skipping batch", region);
                continue;
            }
            
            if (!skipValueCache && valueCache == null) {
                log.warn("Region '{}' value cache not initialized, skipping batch", region);
                continue;
            }
            
            try {
                // CRITICAL: Save to disk FIRST (batch write) before updating index
                log.debug("Calling persistenceStore.saveEntries for {} entries in region '{}'", 
                        regionEntries.size(), region);
                persistenceStore.saveEntries(regionEntries);
                log.debug("persistenceStore.saveEntries completed for region '{}'", region);
                
                // Update KeyIndex for all entries
                // If skipping value cache (autoload mode), mark as PERSISTENCE_ONLY
                // Otherwise mark as BOTH (in memory and on disk)
                ValueLocation location = skipValueCache ? ValueLocation.PERSISTENCE_ONLY : ValueLocation.BOTH;
                
                for (CacheEntry entry : regionEntries) {
                    keyIndex.putFromCacheEntry(entry, location);
                    
                    // Only populate value cache if not skipping (normal mode)
                    if (!skipValueCache) {
                        valueCache.put(entry.getKey(), entry);
                    }
                    
                    savedCount++;
                }
                
                log.debug("Updated KeyIndex for {} entries in region '{}'", regionEntries.size(), region);
                
                // Record statistics
                if (properties.getCache().isEnableStatistics()) {
                    statistics.computeIfAbsent(region, k -> new ConcurrentHashMap<>())
                            .merge(KuberConstants.STAT_SETS, (long) regionEntries.size(), Long::sum);
                }
                metricsService.recordSets(region, regionEntries.size());
                
                if (skipValueCache && log.isDebugEnabled()) {
                    log.debug("Batch saved {} entries to region '{}' (persistence only, skipped value cache)", 
                            regionEntries.size(), region);
                }
                
            } catch (Exception e) {
                log.error("Failed to batch save {} entries to region '{}': {}", 
                        regionEntries.size(), region, e.getMessage(), e);
                // Continue with other regions
            }
        }
        
        log.debug("putEntriesBatch completed, saved {} entries total", savedCount);
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
     * Waits for autoload to complete before clearing.
     * 
     * @param region Region name
     */
    public void clearRegionCaches(String region) {
        waitForRegionLoadingIfNeeded(region, "clearRegionCaches");
        
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
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
    /**
     * Load entries into cache (both KeyIndex and value cache).
     * 
     * <p>OPTIMIZATION (v1.6.1): Respects warm-percentage setting.
     * <ul>
     *   <li>ALL keys are loaded into KeyIndex (for EXISTS/KEYS operations)</li>
     *   <li>Only up to warm-percentage of capacity loaded into value cache</li>
     * </ul>
     * 
     * @param region Region name
     * @param entries Entries to load
     */
    public void loadEntriesIntoCache(String region, List<CacheEntry> entries) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            log.warn("Region '{}' caches not initialized, skipping cache loading", region);
            return;
        }
        
        // Calculate warm limit based on config (v1.6.1)
        int warmPercentage = properties.getAutoload().getWarmPercentage();
        int memoryLimit = getEffectiveMemoryLimit(region);
        int warmLimit = (warmPercentage > 0) ? (int) ((memoryLimit * warmPercentage) / 100.0) : 0;
        
        // Get current cache size
        long currentSize = valueCache.estimatedSize();
        
        for (CacheEntry entry : entries) {
            // Always add key to index
            boolean shouldAddToValueCache = (warmLimit > 0) && (currentSize < warmLimit);
            ValueLocation location = shouldAddToValueCache ? ValueLocation.BOTH : ValueLocation.DISK;
            keyIndex.putFromCacheEntry(entry, location);
            
            // Only add to value cache if under warm limit
            if (shouldAddToValueCache) {
                valueCache.put(entry.getKey(), entry);
                currentSize++;
            }
        }
    }
    
    /**
     * Warm the value cache for a region by loading entries from persistence IN BATCHES.
     * 
     * <p>Uses KeyIndex to find keys that are not in value cache, then loads those
     * specific entries from persistence. This is more memory-efficient than loading
     * all entries at once.
     * 
     * <p>Note: In most cases, explicit warming is not necessary. The cache warms
     * naturally through GET operations which automatically load from persistence
     * when entries are not in memory. This is the recommended approach for large
     * datasets.
     * 
     * @param region Region name to warm
     * @return Number of entries loaded into cache
     */
    public int warmRegionCache(String region) {
        if (!regions.containsKey(region)) {
            log.warn("Cannot warm cache for non-existent region: {}", region);
            return 0;
        }
        
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            log.warn("Region '{}' caches not initialized, cannot warm", region);
            return 0;
        }
        
        // Get the max entries limit for value cache (from config: kuber.cache.max-memory-entries)
        int maxEntries = properties.getCache().getMaxMemoryEntries();
        
        // Get current cache size to determine how many more we can load
        long currentSize = valueCache.estimatedSize();
        int entriesToLoad = Math.max(0, maxEntries - (int) currentSize);
        
        if (entriesToLoad == 0) {
            log.info("Region '{}' value cache already at capacity ({} entries), skipping warm", 
                    region, currentSize);
            return 0;
        }
        
        log.info("Warming cache for region '{}': loading up to {} entries (current: {}, max: {})", 
                region, entriesToLoad, currentSize, maxEntries);
        
        long startTime = System.currentTimeMillis();
        
        // Collect keys to load - iterate directly without creating full key set
        List<String> keysToLoad = new ArrayList<>(Math.min(entriesToLoad, 10000));
        int examined = 0;
        
        // Get keys that are not in value cache
        // Use getKeysOnDiskOnly() which returns keys where value is on disk, not in memory
        List<String> diskOnlyKeys = keyIndex.getKeysOnDiskOnly();
        
        for (String key : diskOnlyKeys) {
            // Stop if we have enough keys
            if (keysToLoad.size() >= entriesToLoad) {
                break;
            }
            
            examined++;
            
            // Double-check not in cache (might have been loaded between calls)
            if (valueCache.get(key) != null) {
                continue;
            }
            
            keysToLoad.add(key);
        }
        
        // Clear reference to allow GC
        diskOnlyKeys = null;
        
        if (keysToLoad.isEmpty()) {
            log.info("No keys to warm for region '{}' (examined {} disk-only keys)", region, examined);
            return 0;
        }
        
        log.info("Found {} keys to warm from {} disk-only keys examined", keysToLoad.size(), examined);
        
        // Load entries in batches to avoid memory issues
        final int BATCH_SIZE = 1000;
        int totalWarmed = 0;
        int batchNumber = 0;
        
        for (int i = 0; i < keysToLoad.size(); i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, keysToLoad.size());
            List<String> batchKeys = keysToLoad.subList(i, end);
            batchNumber++;
            
            // Load this batch of entries from persistence
            Map<String, CacheEntry> entries = persistenceStore.loadEntriesByKeys(region, batchKeys);
            
            int batchWarmed = 0;
            for (Map.Entry<String, CacheEntry> e : entries.entrySet()) {
                CacheEntry entry = e.getValue();
                
                // Skip expired entries
                if (entry.isExpired()) {
                    continue;
                }
                
                // Update KeyIndex to mark as BOTH (in memory and on disk)
                keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
                
                // Put in value cache
                valueCache.put(entry.getKey(), entry);
                batchWarmed++;
                totalWarmed++;
            }
            
            if (batchWarmed > 0 && batchNumber % 10 == 0) {
                log.debug("Warming progress: {} batches, {} entries loaded", batchNumber, totalWarmed);
            }
        }
        
        // Clear reference to allow GC
        keysToLoad.clear();
        
        long elapsedMs = System.currentTimeMillis() - startTime;
        log.info("Cache warming complete for region '{}': loaded {} entries in {} batches ({} ms)", 
                region, totalWarmed, batchNumber, elapsedMs);
        
        return totalWarmed;
    }
    
    /**
     * Warm the value cache for a region with a specific limit on entries to load.
     * 
     * <p>This method is used by reload and restore operations to respect the
     * configured warm percentage, rather than loading up to the full memory limit.
     * 
     * @param region Region name to warm
     * @param maxEntriesToLoad Maximum number of entries to load
     * @return Number of entries actually loaded into cache
     * @since 1.6.1
     */
    public int warmRegionCacheWithLimit(String region, int maxEntriesToLoad) {
        // SAFETY CHECK (v1.6.3): Don't warm during shutdown
        if (shuttingDown) {
            log.info("Warming skipped for region '{}': shutdown in progress", region);
            return 0;
        }
        
        if (!regions.containsKey(region)) {
            log.warn("Cannot warm cache for non-existent region: {}", region);
            return 0;
        }
        
        if (maxEntriesToLoad <= 0) {
            log.info("Warming skipped for region '{}': limit is 0", region);
            return 0;
        }
        
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            log.warn("Region '{}' caches not initialized, cannot warm", region);
            return 0;
        }
        
        // Get current cache size to determine how many more we can load
        long currentSize = valueCache.estimatedSize();
        int entriesToLoad = Math.max(0, maxEntriesToLoad - (int) currentSize);
        
        if (entriesToLoad == 0) {
            log.info("Region '{}' value cache already at warm limit ({} entries), skipping", 
                    region, currentSize);
            return 0;
        }
        
        log.info("Warming cache for region '{}': loading up to {} entries (current: {}, limit: {})", 
                region, entriesToLoad, currentSize, maxEntriesToLoad);
        
        long startTime = System.currentTimeMillis();
        
        // Get keys that are not in value cache
        List<String> diskOnlyKeys = keyIndex.getKeysOnDiskOnly();
        
        // Collect keys to load
        List<String> keysToLoad = new ArrayList<>(Math.min(entriesToLoad, 10000));
        
        for (String key : diskOnlyKeys) {
            if (keysToLoad.size() >= entriesToLoad) {
                break;
            }
            
            // Double-check not in cache
            if (valueCache.get(key) != null) {
                continue;
            }
            
            keysToLoad.add(key);
        }
        
        // Clear reference to allow GC
        diskOnlyKeys = null;
        
        if (keysToLoad.isEmpty()) {
            log.info("No keys to warm for region '{}'", region);
            return 0;
        }
        
        // Load entries in batches
        final int BATCH_SIZE = 1000;
        int totalWarmed = 0;
        
        for (int i = 0; i < keysToLoad.size(); i += BATCH_SIZE) {
            // SAFETY CHECK (v1.6.3): Stop warming if shutdown in progress
            if (shuttingDown) {
                log.info("Warming stopped for region '{}': shutdown in progress (loaded {} so far)", 
                        region, totalWarmed);
                break;
            }
            
            int end = Math.min(i + BATCH_SIZE, keysToLoad.size());
            List<String> batchKeys = keysToLoad.subList(i, end);
            
            Map<String, CacheEntry> entries = persistenceStore.loadEntriesByKeys(region, batchKeys);
            
            for (Map.Entry<String, CacheEntry> e : entries.entrySet()) {
                CacheEntry entry = e.getValue();
                
                if (entry.isExpired()) {
                    continue;
                }
                
                keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
                valueCache.put(entry.getKey(), entry);
                totalWarmed++;
            }
        }
        
        keysToLoad.clear();
        
        long elapsedMs = System.currentTimeMillis() - startTime;
        log.info("Cache warming with limit complete for region '{}': loaded {} entries ({} ms)", 
                region, totalWarmed, elapsedMs);
        
        return totalWarmed;
    }
    
    /**
     * Warm a percentage of the cache capacity in a background thread.
     * 
     * <p>This method starts a background thread to load entries from persistence
     * into the value cache. It does NOT block the calling thread.
     * 
     * <p>The warming process:
     * <ul>
     *   <li>Calculates target entries based on percentage of max-memory-entries</li>
     *   <li>Loads entries in batches of 1000 to avoid memory spikes</li>
     *   <li>Runs with low priority to minimize impact on other operations</li>
     *   <li>Stops if region becomes unavailable or loading starts</li>
     * </ul>
     * 
     * @param region Region name to warm
     * @param percentageOfCapacity Percentage of max-memory-entries to warm (e.g., 25 for 25%)
     */
    public void warmRegionCacheInBackground(String region, int percentageOfCapacity) {
        // SAFETY CHECK (v1.6.3): Don't start new warming during shutdown
        if (shuttingDown) {
            log.info("Background warming skipped for region '{}': shutdown in progress", region);
            return;
        }
        
        if (!regions.containsKey(region)) {
            log.warn("Cannot warm cache for non-existent region: {}", region);
            return;
        }
        
        // Calculate target entries
        int maxEntries = properties.getCache().getMaxMemoryEntries();
        int targetEntries = (int) ((maxEntries * percentageOfCapacity) / 100.0);
        
        if (targetEntries <= 0) {
            log.info("Background warming skipped for region '{}': target entries is 0", region);
            return;
        }
        
        log.info("Starting background cache warming for region '{}': target {} entries ({}% of {})", 
                region, targetEntries, percentageOfCapacity, maxEntries);
        
        Thread warmingThread = new Thread(() -> {
            try {
                backgroundWarmingTask(region, targetEntries);
            } catch (Exception e) {
                log.error("Background warming failed for region '{}': {}", region, e.getMessage());
            }
        }, "kuber-cache-warmer-" + region);
        
        warmingThread.setDaemon(true);
        warmingThread.setPriority(Thread.MIN_PRIORITY); // Low priority
        warmingThread.start();
    }
    
    /**
     * Background warming task - runs in separate thread.
     */
    private void backgroundWarmingTask(String region, int targetEntries) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            log.warn("Region '{}' caches not initialized, cannot warm", region);
            return;
        }
        
        long startTime = System.currentTimeMillis();
        
        // Get current cache size
        long currentSize = valueCache.estimatedSize();
        int entriesToLoad = Math.max(0, targetEntries - (int) currentSize);
        
        if (entriesToLoad <= 0) {
            log.info("Background warming skipped for region '{}': already have {} entries (target: {})", 
                    region, currentSize, targetEntries);
            return;
        }
        
        // Get keys that are not in value cache (disk-only keys)
        List<String> diskOnlyKeys = keyIndex.getKeysOnDiskOnly();
        
        if (diskOnlyKeys.isEmpty()) {
            log.info("Background warming: no disk-only keys for region '{}'", region);
            return;
        }
        
        // Collect keys to load (up to target)
        List<String> keysToLoad = new ArrayList<>(Math.min(entriesToLoad, diskOnlyKeys.size()));
        for (String key : diskOnlyKeys) {
            if (keysToLoad.size() >= entriesToLoad) {
                break;
            }
            // Double-check not in cache
            if (valueCache.get(key) == null) {
                keysToLoad.add(key);
            }
        }
        
        if (keysToLoad.isEmpty()) {
            log.info("Background warming: all keys already in cache for region '{}'", region);
            return;
        }
        
        log.info("Background warming for region '{}': loading {} entries...", region, keysToLoad.size());
        
        // Load in batches with short sleeps to avoid overwhelming the system
        final int BATCH_SIZE = 500; // Smaller batches for background
        int totalWarmed = 0;
        int batchNumber = 0;
        
        for (int i = 0; i < keysToLoad.size(); i += BATCH_SIZE) {
            // SAFETY CHECK (v1.6.3): Stop warming if shutdown in progress
            if (shuttingDown) {
                log.info("Background warming stopped for region '{}': shutdown in progress", region);
                break;
            }
            
            // Check if region is still valid and not being loaded
            if (!regions.containsKey(region) || operationLock.isRegionLoading(region)) {
                log.info("Background warming interrupted for region '{}': region status changed", region);
                break;
            }
            
            int end = Math.min(i + BATCH_SIZE, keysToLoad.size());
            List<String> batchKeys = keysToLoad.subList(i, end);
            batchNumber++;
            
            try {
                Map<String, CacheEntry> entries = persistenceStore.loadEntriesByKeys(region, batchKeys);
                
                for (Map.Entry<String, CacheEntry> e : entries.entrySet()) {
                    CacheEntry entry = e.getValue();
                    
                    if (entry.isExpired()) {
                        continue;
                    }
                    
                    keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
                    valueCache.put(entry.getKey(), entry);
                    totalWarmed++;
                }
                
                // Short sleep between batches to reduce load
                if (i + BATCH_SIZE < keysToLoad.size()) {
                    Thread.sleep(50); // 50ms between batches
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Background warming interrupted for region '{}'", region);
                break;
            } catch (Exception e) {
                log.warn("Background warming batch {} failed for region '{}': {}", 
                        batchNumber, region, e.getMessage());
            }
        }
        
        long elapsedMs = System.currentTimeMillis() - startTime;
        log.info("Background warming complete for region '{}': loaded {} entries in {} batches ({} ms)", 
                region, totalWarmed, batchNumber, elapsedMs);
    }
    
    /**
     * Reload a region's value cache from persistence.
     * 
     * <p>This operation:
     * <ol>
     *   <li>Clears (evicts) all entries from the value cache</li>
     *   <li>Optionally warms the cache by loading entries from persistence</li>
     * </ol>
     * 
     * <p>The KeyIndex is NOT cleared - it remains intact to track which keys exist.
     * After clearing, GET operations will automatically load data from persistence
     * and populate the cache (lazy loading).
     * 
     * <p>Use this when:
     * <ul>
     *   <li>Value cache has stale data and needs refresh</li>
     *   <li>Memory needs to be reclaimed</li>
     *   <li>Cache eviction policy has removed important entries</li>
     * </ul>
     * 
     * @param region Region name
     * @return Number of entries loaded into cache after reload (0 if no explicit warming)
     * @throws IllegalStateException if region doesn't exist or is being loaded/restored
     */
    public int reloadRegionFromPersistence(String region) {
        if (!regions.containsKey(region)) {
            throw new IllegalStateException("Region '" + region + "' does not exist");
        }
        
        // Wait for any ongoing autoload to complete
        waitForRegionLoadingIfNeeded(region, "reloadFromPersistence");
        
        // Check if region is being restored
        checkRegionNotBeingRestored(region);
        
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        KeyIndexInterface keyIndex = keyIndices.get(region);
        
        if (valueCache == null || keyIndex == null) {
            throw new IllegalStateException("Region '" + region + "' caches not initialized");
        }
        
        log.info("Reloading region '{}' from persistence: clearing value cache...", region);
        
        long entriesBefore = valueCache.estimatedSize();
        
        // Step 1: Clear the value cache (evict all entries)
        // This is safe and fast - Caffeine handles it efficiently
        valueCache.invalidateAll();
        
        log.info("Cleared {} entries from value cache for region '{}'", entriesBefore, region);
        
        // Step 2: Warm the cache using the configured warm percentage (v1.6.1)
        // This respects the same limit as startup/autoload for consistency
        // If warm-percentage=0, no entries will be loaded (lazy loading only)
        int warmPercentage = properties.getAutoload().getWarmPercentage();
        int warmedCount = 0;
        
        if (warmPercentage > 0) {
            // Calculate the warm limit based on memory limit and warm percentage
            int memoryLimit = getEffectiveMemoryLimit(region);
            int warmLimit = (int) ((memoryLimit * warmPercentage) / 100.0);
            warmedCount = warmRegionCacheWithLimit(region, warmLimit);
            
            log.info("Region '{}' reload complete: evicted {} entries, warmed {} entries ({}% of limit {})", 
                    region, entriesBefore, warmedCount, warmPercentage, memoryLimit);
        } else {
            log.info("Region '{}' reload complete: evicted {} entries, warming disabled (warm-percentage=0)", 
                    region, entriesBefore);
        }
        
        log.info("Remaining entries will be loaded on-demand via GET operations (lazy loading)");
        
        return warmedCount;
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
     * Wait for region autoload to complete if in progress.
     * Used by write operations and stats collection to ensure data consistency during autoload.
     * 
     * <p>Write operations (put, set, delete, etc.) must wait for autoload to complete
     * to prevent data inconsistency. Stats collection also waits to ensure accurate counts.
     * Read operations can proceed during autoload (reads may return stale or missing data
     * which is acceptable).
     * 
     * @param region Region name
     * @param operationName Name of the operation (for logging)
     * @throws IllegalStateException if timeout waiting for autoload to complete
     */
    private void waitForRegionLoadingIfNeeded(String region, String operationName) {
        if (operationLock.isRegionLoading(region)) {
            log.debug("Region '{}' is loading, waiting for completion before {}...", region, operationName);
            if (!operationLock.waitForRegionReady(region, 60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timeout waiting for autoload to complete on region '" + region + 
                        "'. Operation '" + operationName + "' cannot proceed. Please try again later.");
            }
            log.debug("Region '{}' autoload complete, proceeding with {}", region, operationName);
        }
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
    
    // ==================== REPLICATION APPLY METHODS ====================
    // These methods are called by ReplicationSyncService on SECONDARY nodes.
    // They bypass checkWriteAccess() because the data comes from the PRIMARY
    // via the replication protocol, not from external clients.
    // They also skip oplog append (to avoid feedback loops).
    
    /**
     * Apply a replicated SET operation from the PRIMARY.
     * Updates KeyIndex, value cache, and persistence without write-access check.
     */
    public void applyReplicatedSet(String region, String key, CacheEntry entry) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            log.warn("Replication: cannot apply SET for unknown region '{}', key '{}'", region, key);
            return;
        }
        
        // Update KeyIndex
        keyIndex.putFromCacheEntry(entry, ValueLocation.BOTH);
        
        // Update value cache
        valueCache.put(key, entry);
        
        // Persist asynchronously
        persistenceStore.saveEntryAsync(entry);
        
        recordStatistic(region, KuberConstants.STAT_SETS);
    }
    
    /**
     * Apply a replicated DELETE operation from the PRIMARY.
     */
    public void applyReplicatedDelete(String region, String key) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            log.debug("Replication: cannot apply DELETE for unknown region '{}'", region);
            return;
        }
        
        keyIndex.remove(key);
        valueCache.invalidate(key);
        
        try {
            persistenceStore.deleteEntry(region, key);
        } catch (Exception e) {
            // Entry may not exist on disk yet
        }
        
        recordStatistic(region, KuberConstants.STAT_DELETES);
    }
    
    /**
     * Apply a replicated REGION_CREATE operation from the PRIMARY.
     * Creates the region locally without write-access check.
     */
    public void applyReplicatedRegionCreate(String name, String description) {
        if (regions.containsKey(name)) {
            log.debug("Replication: region '{}' already exists, skipping create", name);
            return;
        }
        
        String collectionName = "kuber_" + name.toLowerCase();
        
        CacheRegion region = CacheRegion.builder()
                .name(name)
                .description(description)
                .collectionName(collectionName)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        
        regions.put(name, region);
        calculateMemoryLimitForNewRegion(name);
        createCacheForRegion(name);
        metricsService.registerRegion(name);
        
        try {
            persistenceStore.saveRegion(region);
        } catch (Exception e) {
            log.warn("Replication: failed to persist replicated region '{}': {}", name, e.getMessage());
        }
        
        log.info("Replication: created region '{}'", name);
    }
    
    /**
     * Apply a replicated REGION_DELETE operation from the PRIMARY.
     */
    public void applyReplicatedRegionDelete(String name) {
        CacheRegion region = regions.get(name);
        if (region == null) {
            log.debug("Replication: region '{}' not found for delete, skipping", name);
            return;
        }
        
        keyIndices.remove(name);
        CacheProxy<String, CacheEntry> cache = regionCaches.remove(name);
        if (cache != null) {
            cache.invalidateAll();
        }
        regions.remove(name);
        statistics.remove(name);
        
        try {
            persistenceStore.deleteRegion(name);
        } catch (Exception e) {
            log.warn("Replication: failed to delete region '{}' from persistence: {}", name, e.getMessage());
        }
        
        log.info("Replication: deleted region '{}'", name);
    }
    
    /**
     * Apply a replicated REGION_PURGE operation from the PRIMARY.
     */
    public void applyReplicatedRegionPurge(String name) {
        CacheRegion region = regions.get(name);
        if (region == null) {
            log.debug("Replication: region '{}' not found for purge, skipping", name);
            return;
        }
        
        KeyIndexInterface keyIndex = keyIndices.get(name);
        if (keyIndex != null) {
            keyIndex.clear();
        }
        
        CacheProxy<String, CacheEntry> cache = regionCaches.get(name);
        if (cache != null) {
            cache.invalidateAll();
        }
        
        region.setEntryCount(0);
        region.setUpdatedAt(Instant.now());
        
        persistenceStore.purgeRegion(name);
        
        log.info("Replication: purged region '{}'", name);
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
        
        CacheProxy<String, CacheEntry> cache = regionCaches.get(region);
        if (cache != null) {
            stats.put("memoryEntries", cache.estimatedSize());
            CacheProxy.CacheStats cacheStats = cache.stats();
            if (cacheStats != null) {
                stats.put("cacheStats", String.format("hits=%d, misses=%d, hitRate=%.2f%%",
                        cacheStats.hitCount(), cacheStats.missCount(), cacheStats.hitRate() * 100));
            }
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
        info.put("version", properties.getVersion());
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
                .mapToLong(CacheProxy::estimatedSize)
                .sum();
        info.put("valuesInMemory", totalMemoryValues);
        
        // Cache implementation info (v1.5.0)
        info.put("cacheImplementation", factoryProvider.getCacheFactory().getType());
        info.put("collectionsImplementation", factoryProvider.getCollectionsFactory().getType());
        
        // Cold values (on disk only)
        long valuesOnDiskOnly = keyIndices.values().stream()
                .mapToLong(idx -> idx.getKeysOnDiskOnly().size())
                .sum();
        info.put("valuesOnDiskOnly", valuesOnDiskOnly);
        
        return info;
    }
    
    /**
     * Get database size - O(1) using KeyIndex (NEVER hits disk).
     * Waits for autoload to complete to ensure accurate counts.
     */
    public long dbSize(String region) {
        if (region == null || region.isEmpty()) {
            // Return total across all regions from KeyIndex - O(1) per region
            // For total count, we wait for all loading regions
            for (String r : keyIndices.keySet()) {
                waitForRegionLoadingIfNeeded(r, "dbSize total");
            }
            return keyIndices.values().stream()
                    .mapToLong(KeyIndexInterface::size)
                    .sum();
        }
        
        // Wait for this specific region to finish loading
        waitForRegionLoadingIfNeeded(region, "dbSize");
        
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
     * Get in-memory value count for a region (values in value cache).
     */
    public long getMemoryEntryCount(String region) {
        CacheProxy<String, CacheEntry> cache = regionCaches.get(region);
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
    
    /**
     * Refresh stats for a specific region.
     * Recalculates entry counts from KeyIndex (source of truth) and updates CacheRegion.
     * Also verifies consistency with persistence store.
     * 
     * @param regionName Region name
     * @return Updated stats for the region
     */
    public Map<String, Object> refreshRegionStats(String regionName) {
        CacheRegion region = regions.get(regionName);
        if (region == null) {
            throw RegionException.notFound(regionName);
        }
        
        KeyIndexInterface keyIndex = keyIndices.get(regionName);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(regionName);
        
        // Get counts from sources of truth
        long keyIndexCount = keyIndex != null ? keyIndex.size() : 0;
        long valueCacheCount = valueCache != null ? valueCache.estimatedSize() : 0;
        long persistenceEstimate = persistenceStore.estimateEntryCount(regionName);
        
        // Update CacheRegion with accurate counts
        region.setEntryCount(keyIndexCount);
        region.setMemoryEntryCount(valueCacheCount);
        region.setPersistenceEntryCount(keyIndexCount); // KeyIndex = source of truth
        region.setUpdatedAt(Instant.now());
        
        // Build result
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("region", regionName);
        stats.put("entryCount", keyIndexCount);
        stats.put("memoryEntries", valueCacheCount);
        stats.put("persistenceEntries", keyIndexCount);
        stats.put("persistenceEstimate", persistenceEstimate);
        stats.put("keyIndexAvailable", keyIndex != null);
        stats.put("valueCacheAvailable", valueCache != null);
        stats.put("updatedAt", region.getUpdatedAt());
        
        // Check for discrepancy (for diagnostics)
        if (persistenceEstimate > 0 && keyIndexCount == 0) {
            stats.put("warning", "KeyIndex is empty but persistence store reports entries. " +
                    "Region may need to be reloaded from persistence.");
            log.warn("[{}] Stats discrepancy: KeyIndex={}, PersistenceEstimate={}", 
                    regionName, keyIndexCount, persistenceEstimate);
        }
        
        log.info("[{}] Stats refreshed: KeyIndex={}, ValueCache={}, PersistenceEstimate={}", 
                regionName, keyIndexCount, valueCacheCount, persistenceEstimate);
        
        return stats;
    }
    
    /**
     * Refresh stats for all regions.
     * 
     * @return List of stats for all regions
     */
    public List<Map<String, Object>> refreshAllRegionStats() {
        List<Map<String, Object>> allStats = new ArrayList<>();
        for (String regionName : regions.keySet()) {
            try {
                allStats.add(refreshRegionStats(regionName));
            } catch (Exception e) {
                Map<String, Object> errorStats = new LinkedHashMap<>();
                errorStats.put("region", regionName);
                errorStats.put("error", e.getMessage());
                allStats.add(errorStats);
            }
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
        for (Map.Entry<String, CacheProxy<String, CacheEntry>> regionEntry : regionCaches.entrySet()) {
            if (totalEvicted >= count) {
                break;
            }
            
            String regionName = regionEntry.getKey();
            CacheProxy<String, CacheEntry> valueCache = regionEntry.getValue();
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
            
            // v2.2.0: Increment eviction counter for periodic summary logging
            if (!keysToEvict.isEmpty()) {
                evictionCounters.computeIfAbsent(regionName, k -> new AtomicLong(0))
                        .addAndGet(keysToEvict.size());
            }
        }
        
        return totalEvicted;
    }
    
    /**
     * Evict values from a specific region.
     * Used by ValueCacheLimitService for count-based eviction.
     * 
     * @param region Region name
     * @param count Maximum number of values to evict
     * @return Actual number of values evicted
     * @since 1.7.4
     */
    public int evictValuesFromRegion(String region, int count) {
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        KeyIndexInterface keyIndex = keyIndices.get(region);
        
        if (valueCache == null) {
            return 0;
        }
        
        int evicted = 0;
        List<String> keysToEvict = new ArrayList<>();
        List<CacheEntry> entriesToPersist = new ArrayList<>();
        
        // Collect entries to evict (LRU order from cache)
        for (Map.Entry<String, CacheEntry> entry : valueCache.asMap().entrySet()) {
            if (keysToEvict.size() >= count) {
                break;
            }
            
            CacheEntry cacheEntry = entry.getValue();
            
            // Skip expired entries
            if (cacheEntry.isExpired()) {
                continue;
            }
            
            keysToEvict.add(entry.getKey());
            entriesToPersist.add(cacheEntry);
        }
        
        // Persist entries before evicting
        for (CacheEntry entry : entriesToPersist) {
            try {
                persistenceStore.saveEntry(entry);
            } catch (Exception e) {
                log.warn("Failed to persist entry '{}' during count-based eviction: {}", 
                        entry.getKey(), e.getMessage());
            }
        }
        
        // Evict values from memory and update KeyIndex
        for (String key : keysToEvict) {
            valueCache.invalidate(key);
            
            // Update KeyIndex to mark value as DISK only
            if (keyIndex != null) {
                keyIndex.updateLocation(key, ValueLocation.DISK);
            }
            
            evicted++;
            recordStatistic(region, "evictions");
        }
        
        // v2.2.0: Increment eviction counter for periodic summary logging
        if (evicted > 0) {
            evictionCounters.computeIfAbsent(region, k -> new AtomicLong(0))
                    .addAndGet(evicted);
        }
        
        return evicted;
    }
    
    /**
     * Get total number of keys in a region (from KeyIndex).
     * 
     * @param region Region name
     * @return Total key count in the region
     * @since 1.7.4
     */
    public long getKeyCount(String region) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        if (keyIndex != null) {
            return keyIndex.size();
        }
        return 0;
    }
    
    /**
     * Get number of values currently in memory for a region.
     * 
     * @param region Region name
     * @return Number of values in memory (value cache size)
     * @since 1.7.4
     */
    public long getValueCacheSize(String region) {
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        if (valueCache != null) {
            return valueCache.estimatedSize();
        }
        return 0;
    }
    
    /**
     * Get keys that are in the key index but NOT in the value cache (cold keys).
     * These are keys whose values are on disk only.
     * Used by WarmObjectService to identify keys that need to be warmed.
     * 
     * @param region Region name
     * @param maxCount Maximum number of cold keys to return
     * @return List of cold key names
     * @since 1.7.9
     */
    public List<String> getColdKeys(String region, int maxCount) {
        KeyIndexInterface keyIndex = keyIndices.get(region);
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        
        if (keyIndex == null || valueCache == null) {
            return Collections.emptyList();
        }
        
        List<String> coldKeys = new ArrayList<>();
        Set<String> warmKeys = valueCache.asMap().keySet();
        
        // Iterate through all keys in the index
        for (String key : keyIndex.getAllKeys()) {
            if (!warmKeys.contains(key)) {
                coldKeys.add(key);
                if (coldKeys.size() >= maxCount) {
                    break;
                }
            }
        }
        
        return coldKeys;
    }
    
    /**
     * Warm an object by putting it into the value cache.
     * Used by WarmObjectService to load objects from disk into memory.
     * 
     * @param region Region name
     * @param entry The cache entry to warm
     * @since 1.7.9
     */
    public void warmObject(String region, CacheEntry entry) {
        if (entry == null || entry.isExpired()) {
            return;
        }
        
        CacheProxy<String, CacheEntry> valueCache = regionCaches.get(region);
        if (valueCache == null) {
            return;
        }
        
        // Record access time
        entry.recordAccess();
        
        // Put into value cache
        valueCache.put(entry.getKey(), entry);
        
        // Update key index to mark as in-memory
        KeyIndexInterface keyIndex = keyIndices.get(region);
        if (keyIndex != null) {
            keyIndex.updateLocation(entry.getKey(), KeyIndexEntry.ValueLocation.MEMORY);
        }
    }
    
    /**
     * Get total number of entries across all regions in memory.
     */
    public long getTotalMemoryEntries() {
        return regionCaches.values().stream()
                .mapToLong(CacheProxy::estimatedSize)
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
        
        for (Map.Entry<String, CacheProxy<String, CacheEntry>> entry : regionCaches.entrySet()) {
            String regionName = entry.getKey();
            CacheProxy<String, CacheEntry> valueCache = entry.getValue();
            KeyIndexInterface keyIndex = keyIndices.get(regionName);
            
            // Collect expired keys with their values (for event publishing with payload)
            Map<String, String> expiredKeyValues = new LinkedHashMap<>();
            
            // Check value cache for expired entries - capture value before eviction
            valueCache.asMap().forEach((key, cacheEntry) -> {
                if (cacheEntry.isExpired()) {
                    expiredKeyValues.put(key, cacheEntry.getStringValue());
                }
            });
            
            // Also check KeyIndex for expired entries (cold values on disk only)
            if (keyIndex != null) {
                for (String key : keyIndex.findExpiredKeys()) {
                    if (!expiredKeyValues.containsKey(key)) {
                        // Disk-only entry: try to read value from persistence before deleting
                        String diskValue = null;
                        try {
                            CacheEntry diskEntry = persistenceStore.loadEntry(regionName, key);
                            if (diskEntry != null) {
                                diskValue = diskEntry.getStringValue();
                            }
                        } catch (Exception e) {
                            log.debug("Could not read value for expired disk-only key '{}': {}", key, e.getMessage());
                        }
                        expiredKeyValues.put(key, diskValue);
                    }
                }
            }
            
            for (Map.Entry<String, String> kvEntry : expiredKeyValues.entrySet()) {
                String key = kvEntry.getKey();
                String expiredValue = kvEntry.getValue();
                
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
                
                // v2.6.0: Publish expired event with payload (skip if Caffeine listener already published)
                String compositeKey = regionName + "|" + key;
                if (recentlyPublishedExpired.remove(compositeKey)) {
                    // Already published by Caffeine removal listener with full value — skip
                    log.debug("Expired event already published for key '{}' in region '{}'", key, regionName);
                } else {
                    // Publish with value payload (mimics insert/update event structure)
                    eventPublisher.publish(CacheEvent.entryExpired(regionName, key, properties.getNodeId()));
                    if (publishingService != null) {
                        publishingService.publishExpire(regionName, key, expiredValue, properties.getNodeId());
                    }
                }
                
                recordStatistic(regionName, KuberConstants.STAT_EXPIRED);
            }
            
            if (!expiredKeyValues.isEmpty()) {
                log.info("Cleaned up {} expired entries from region '{}' (removed from index and cache)", 
                        expiredKeyValues.size(), regionName);
            }
        }
        
        // Clear any stale entries from the dedup set (safety net)
        recentlyPublishedExpired.clear();
    }
    
    // ==================== Eviction Summary Logging (v2.2.0) ====================
    
    /**
     * Log eviction summary every minute.
     * Only logs at INFO level when there are evictions.
     * 
     * @since 2.0.0
     */
    @Scheduled(fixedRate = 60000)  // Every 1 minute
    public void logEvictionSummary() {
        // Skip if not initialized or shutting down
        if (!initialized.get() || shuttingDown) {
            return;
        }
        
        // Collect and reset counters atomically
        Map<String, Long> evictions = new HashMap<>();
        long totalEvictions = 0;
        
        for (Map.Entry<String, AtomicLong> entry : evictionCounters.entrySet()) {
            long count = entry.getValue().getAndSet(0);
            if (count > 0) {
                evictions.put(entry.getKey(), count);
                totalEvictions += count;
            }
        }
        
        // Log summary - INFO for evictions, DEBUG for no evictions
        if (!evictions.isEmpty()) {
            StringBuilder sb = new StringBuilder("Cache eviction summary (last 60s): ");
            evictions.forEach((region, count) -> 
                sb.append(region).append("=").append(count).append(" "));
            sb.append("[total=").append(totalEvictions).append("]");
            log.info(sb.toString().trim());
        } else {
            log.debug("Cache eviction summary (last 60s): no evictions");
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
     *   <li>Invalidate all value caches (no removal listener callbacks due to shutdown flag)</li>
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
        // - cache removal listeners
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
            for (Map.Entry<String, CacheProxy<String, CacheEntry>> entry : regionCaches.entrySet()) {
                String regionName = entry.getKey();
                CacheProxy<String, CacheEntry> cache = entry.getValue();
                
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
            
            // Step 3b: CRITICAL - Force sync to ensure all batch writes are durable (v1.6.1)
            // CacheService.shutdown() may be called without going through ShutdownOrchestrator
            // (e.g., direct call or emergency shutdown), so we must sync here explicitly
            log.info("  Step 3b: Forcing sync to ensure all writes are durable...");
            try {
                persistenceStore.sync();
                log.info("    Sync complete - all entries durable on disk");
            } catch (Exception e) {
                log.error("    Failed to sync persistence store: {}", e.getMessage());
            }
            
            // Step 4: Invalidate all value caches
            // This won't trigger removal listeners because shuttingDown=true
            log.info("  Step 4: Invalidating {} value caches...", regionCaches.size());
            for (Map.Entry<String, CacheProxy<String, CacheEntry>> entry : regionCaches.entrySet()) {
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
