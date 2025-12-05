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
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.event.EventPublisher;
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.replication.ReplicationManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Main cache service providing all cache operations.
 * Supports regions, TTL, JSON queries, and replication.
 */
@Slf4j
@Service
public class CacheService {
    
    private final KuberProperties properties;
    private final PersistenceStore persistenceStore;
    private final EventPublisher eventPublisher;
    private final CacheMetricsService metricsService;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // In-memory cache per region
    private final Map<String, Cache<String, CacheEntry>> regionCaches = new ConcurrentHashMap<>();
    
    // Region metadata
    private final Map<String, CacheRegion> regions = new ConcurrentHashMap<>();
    
    // Effective memory limits per region (calculated considering global cap)
    private final Map<String, Integer> effectiveMemoryLimits = new ConcurrentHashMap<>();
    
    // Statistics
    private final Map<String, Map<String, Long>> statistics = new ConcurrentHashMap<>();
    
    public CacheService(KuberProperties properties, 
                        PersistenceStore persistenceStore,
                        EventPublisher eventPublisher,
                        CacheMetricsService metricsService) {
        this.properties = properties;
        this.persistenceStore = persistenceStore;
        this.eventPublisher = eventPublisher;
        this.metricsService = metricsService;
    }
    
    @PostConstruct
    public void initialize() {
        log.info("Initializing Kuber cache service...");
        log.info("Using persistence store: {}", persistenceStore.getType());
        
        // Load regions from persistence store
        loadRegions();
        
        // Ensure default region exists
        if (!regions.containsKey(KuberConstants.DEFAULT_REGION)) {
            createDefaultRegion();
        }
        
        // Calculate memory allocation for all regions
        calculateMemoryAllocation();
        
        // Create caches with calculated limits
        for (String regionName : regions.keySet()) {
            createCacheForRegion(regionName);
        }
        
        // Prime cache from persistence store
        primeCache();
        
        log.info("Cache service initialized with {} regions", regions.size());
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
        
        Cache<String, CacheEntry> cache = Caffeine.newBuilder()
                .maximumSize(memoryLimit)
                .expireAfterWrite(24, TimeUnit.HOURS)
                .removalListener((key, value, cause) -> {
                    // Only track statistics - don't modify entryCount since persistence is the source of truth
                    // Entries evicted from memory cache are still in persistence store
                    if (cause == RemovalCause.SIZE || cause == RemovalCause.EXPIRED) {
                        recordStatistic(regionName, KuberConstants.STAT_EVICTED);
                        log.debug("Entry '{}' evicted from memory cache in region '{}' due to {} (still in persistence)", 
                                key, regionName, cause);
                    }
                })
                .recordStats()
                .build();
        
        regionCaches.put(regionName, cache);
        statistics.put(regionName, new ConcurrentHashMap<>());
    }
    
    private void primeCache() {
        log.info("Priming cache from {} persistence store (loading most recently accessed entries first)...", 
                persistenceStore.getType());
        
        int totalPrimed = 0;
        
        for (String regionName : regions.keySet()) {
            try {
                int memoryLimit = getEffectiveMemoryLimit(regionName);
                long persistedCount = persistenceStore.countNonExpiredEntries(regionName);
                List<CacheEntry> entries = persistenceStore.loadEntries(regionName, memoryLimit);
                Cache<String, CacheEntry> cache = regionCaches.get(regionName);
                
                if (cache == null) {
                    log.warn("Cache not found for region '{}', skipping priming", regionName);
                    continue;
                }
                
                int primedCount = 0;
                for (CacheEntry entry : entries) {
                    if (!entry.isExpired()) {
                        cache.put(entry.getKey(), entry);
                        primedCount++;
                    }
                }
                
                totalPrimed += primedCount;
                
                if (persistedCount > memoryLimit) {
                    log.info("Primed {} entries for region '{}' (loaded most recent {} of {} persisted, limit={})", 
                            primedCount, regionName, memoryLimit, persistedCount, memoryLimit);
                } else {
                    log.info("Primed {} entries for region '{}' (all {} persisted entries loaded, limit={})", 
                            primedCount, regionName, persistedCount, memoryLimit);
                }
            } catch (Exception e) {
                log.warn("Failed to prime cache for region '{}': {}", regionName, e.getMessage());
            }
        }
        
        int totalAllocated = effectiveMemoryLimits.values().stream().mapToInt(Integer::intValue).sum();
        log.info("Cache priming complete: {} entries loaded into memory across {} regions (total memory allocation: {})", 
                totalPrimed, regions.size(), totalAllocated);
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
        
        // Clear the cache
        Cache<String, CacheEntry> cache = regionCaches.remove(name);
        if (cache != null) {
            cache.invalidateAll();
        }
        
        regions.remove(name);
        statistics.remove(name);
        
        // Delete from persistence store
        persistenceStore.deleteRegion(name);
        
        eventPublisher.publish(CacheEvent.regionDeleted(name, properties.getNodeId()));
        
        log.info("Deleted region: {}", name);
    }
    
    public void purgeRegion(String name) {
        checkWriteAccess();
        
        CacheRegion region = regions.get(name);
        if (region == null) {
            throw RegionException.notFound(name);
        }
        
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
        
        log.info("Purged region: {}", name);
    }
    
    public Collection<CacheRegion> getAllRegions() {
        // Enrich regions with dynamically calculated counts
        // entryCount should reflect persistence (source of truth)
        for (CacheRegion region : regions.values()) {
            long memoryCount = getMemoryEntryCount(region.getName());
            long persistenceCount = getPersistenceEntryCount(region.getName());
            region.setMemoryEntryCount(memoryCount);
            region.setPersistenceEntryCount(persistenceCount);
            // Total entries = persistence count (source of truth)
            region.setEntryCount(persistenceCount);
        }
        return Collections.unmodifiableCollection(regions.values());
    }
    
    public CacheRegion getRegion(String name) {
        CacheRegion region = regions.get(name);
        if (region != null) {
            // Enrich with current counts
            long memoryCount = getMemoryEntryCount(name);
            long persistenceCount = getPersistenceEntryCount(name);
            region.setMemoryEntryCount(memoryCount);
            region.setPersistenceEntryCount(persistenceCount);
            // Total entries = persistence count (source of truth)
            region.setEntryCount(persistenceCount);
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
        ensureRegionExists(region);
        
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
    
    public List<String> mget(String region, List<String> keys) {
        List<String> values = new ArrayList<>();
        for (String key : keys) {
            values.add(get(region, key));
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
        
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        boolean existedInMemory = cache.getIfPresent(key) != null;
        
        // Always invalidate from memory if present
        cache.invalidate(key);
        
        // Always try to delete from persistence (entry might be in persistence but not memory)
        boolean deletedFromPersistence = false;
        try {
            persistenceStore.deleteEntry(region, key);
            deletedFromPersistence = true;
        } catch (Exception e) {
            log.debug("Entry '{}' not found in persistence for region '{}'", key, region);
        }
        
        // Note: entryCount is dynamically calculated from persistence, so no manual decrement needed
        
        if (existedInMemory || deletedFromPersistence) {
            recordStatistic(region, KuberConstants.STAT_DELETES);
            metricsService.recordDelete(region);
            eventPublisher.publish(CacheEvent.entryDeleted(region, key, properties.getNodeId()));
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
    
    public boolean exists(String region, String key) {
        CacheEntry entry = getEntry(region, key);
        return entry != null;
    }
    
    public long exists(String region, List<String> keys) {
        return keys.stream().filter(key -> exists(region, key)).count();
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
    
    public Set<String> keys(String region, String pattern) {
        ensureRegionExists(region);
        
        Pattern regex = globToRegex(pattern);
        Set<String> allKeys = new HashSet<>();
        
        // Get non-expired keys from in-memory cache
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        cache.asMap().entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .map(Map.Entry::getKey)
                .filter(key -> regex.matcher(key).matches())
                .forEach(allKeys::add);
        
        // Get non-expired keys from persistence store
        List<String> persistenceKeys = persistenceStore.getNonExpiredKeys(region, pattern, Integer.MAX_VALUE);
        persistenceKeys.stream()
                .filter(key -> regex.matcher(key).matches())
                .forEach(allKeys::add);
        
        return allKeys;
    }
    
    /**
     * Get keys with a limit - useful for large datasets.
     * Combines non-expired keys from both memory and persistence.
     */
    public Set<String> keys(String region, String pattern, int limit) {
        ensureRegionExists(region);
        
        Pattern regex = globToRegex(pattern);
        Set<String> allKeys = new LinkedHashSet<>(); // Maintain insertion order
        
        // Get non-expired keys from persistence store first (since it likely has more)
        List<String> persistenceKeys = persistenceStore.getNonExpiredKeys(region, pattern, limit);
        persistenceKeys.stream()
                .filter(key -> regex.matcher(key).matches())
                .forEach(allKeys::add);
        
        // Add non-expired keys from in-memory cache
        if (allKeys.size() < limit) {
            Cache<String, CacheEntry> cache = regionCaches.get(region);
            cache.asMap().entrySet().stream()
                    .filter(e -> !e.getValue().isExpired())
                    .map(Map.Entry::getKey)
                    .filter(key -> regex.matcher(key).matches())
                    .filter(key -> !allKeys.contains(key))
                    .limit(limit - allKeys.size())
                    .forEach(allKeys::add);
        }
        
        return allKeys;
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
    
    private CacheEntry getEntry(String region, String key) {
        ensureRegionExists(region);
        
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        CacheEntry entry = cache.getIfPresent(key);
        
        if (entry == null) {
            // Try loading from persistence store
            entry = persistenceStore.loadEntry(region, key);
            if (entry != null && !entry.isExpired()) {
                cache.put(key, entry);
            } else {
                entry = null;
            }
        }
        
        if (entry != null) {
            if (entry.isExpired()) {
                cache.invalidate(key);
                
                // Delete from persistence store
                // Note: entryCount is dynamically calculated from persistence, so no manual decrement needed
                try {
                    persistenceStore.deleteEntry(region, key);
                } catch (Exception e) {
                    log.warn("Failed to delete expired entry '{}' from persistence store: {}", key, e.getMessage());
                }
                
                recordStatistic(region, KuberConstants.STAT_EXPIRED);
                metricsService.recordGet(region, false);
                return null;
            }
            
            entry.recordAccess();
            regions.get(region).recordHit();
            recordStatistic(region, KuberConstants.STAT_HITS);
            metricsService.recordGet(region, true);
        } else {
            regions.get(region).recordMiss();
            recordStatistic(region, KuberConstants.STAT_MISSES);
            metricsService.recordGet(region, false);
        }
        
        return entry;
    }
    
    private void putEntry(String region, String key, CacheEntry entry) {
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        
        cache.put(key, entry);
        
        // Note: entryCount is dynamically calculated from persistence, so no manual increment needed
        
        recordStatistic(region, KuberConstants.STAT_SETS);
        metricsService.recordSet(region);
        
        // Persist to persistence store
        if (properties.getCache().isPersistentMode()) {
            persistenceStore.saveEntry(entry);
        } else {
            persistenceStore.saveEntryAsync(entry);
        }
    }
    
    private void ensureRegionExists(String region) {
        if (!regions.containsKey(region)) {
            // Auto-create region
            createRegion(region, "Auto-created region");
        }
    }
    
    private void checkWriteAccess() {
        if (replicationManager != null && !replicationManager.isPrimary()) {
            throw new ReadOnlyException();
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
        info.put("version", "1.1.13");
        info.put("regionCount", regions.size());
        info.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        info.put("maxMemoryEntries", properties.getCache().getMaxMemoryEntries());
        info.put("persistentMode", properties.getCache().isPersistentMode());
        
        // Use fast estimates for dashboard display (O(1) instead of O(n))
        long totalPersistenceEstimate = regions.keySet().stream()
                .mapToLong(r -> persistenceStore.estimateEntryCount(r))
                .sum();
        info.put("totalEntries", totalPersistenceEstimate);
        
        // In-memory count is already fast
        long totalMemoryEntries = regionCaches.values().stream()
                .mapToLong(Cache::estimatedSize)
                .sum();
        info.put("memoryEntries", totalMemoryEntries);
        info.put("persistenceEntries", totalPersistenceEstimate);
        
        return info;
    }
    
    public long dbSize(String region) {
        if (region == null || region.isEmpty()) {
            // Return fast estimate total across all regions
            return regions.keySet().stream()
                    .mapToLong(r -> persistenceStore.estimateEntryCount(r))
                    .sum();
        }
        
        // Return fast estimate for the region
        return persistenceStore.estimateEntryCount(region);
    }
    
    /**
     * Get the metrics service for monitoring.
     */
    public CacheMetricsService getMetricsService() {
        return metricsService;
    }
    
    /**
     * Get in-memory entry count for a region.
     */
    public long getMemoryEntryCount(String region) {
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        return cache != null ? cache.estimatedSize() : 0;
    }
    
    /**
     * Get persistence store entry count for a region (excludes expired entries).
     * WARNING: This is slow O(n) - iterates all entries. Use for accurate counts only.
     * For dashboard/UI display, use getEstimatedPersistenceEntryCount() instead.
     */
    public long getPersistenceEntryCount(String region) {
        return persistenceStore.countNonExpiredEntries(region);
    }
    
    /**
     * Get fast estimated entry count for a region from persistence store.
     * This is O(1) and suitable for dashboard display.
     */
    public long getEstimatedPersistenceEntryCount(String region) {
        return persistenceStore.estimateEntryCount(region);
    }
    
    /**
     * Get detailed region statistics including memory vs persistence.
     * Uses fast estimates for dashboard display.
     */
    public Map<String, Object> getDetailedRegionStats(String region) {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("region", region);
        
        long memoryEntries = getMemoryEntryCount(region);
        long persistenceEstimate = persistenceStore.estimateEntryCount(region);
        
        CacheRegion regionObj = regions.get(region);
        if (regionObj != null) {
            stats.put("hitRatio", String.format("%.2f", regionObj.getHitRatio()));
        }
        
        // Use fast estimates for dashboard - O(1) instead of O(n)
        stats.put("entryCount", persistenceEstimate);
        stats.put("memoryEntries", memoryEntries);
        stats.put("persistenceEntries", persistenceEstimate);
        
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
     * Evict entries from memory to persistence store.
     * Used by MemoryWatcherService to reduce heap usage under memory pressure.
     * Entries are persisted before being removed from memory to prevent data loss.
     * 
     * @param count Maximum number of entries to evict
     * @return Actual number of entries evicted
     */
    public int evictEntriesToPersistence(int count) {
        int totalEvicted = 0;
        
        // Iterate through regions and evict entries
        for (Map.Entry<String, Cache<String, CacheEntry>> regionEntry : regionCaches.entrySet()) {
            if (totalEvicted >= count) {
                break;
            }
            
            String regionName = regionEntry.getKey();
            Cache<String, CacheEntry> cache = regionEntry.getValue();
            CacheRegion region = regions.get(regionName);
            
            // Get entries to evict (Caffeine's asMap() returns in no particular order,
            // but the cache is configured with LRU so entries accessed less recently
            // will be candidates for eviction)
            int regionEvictCount = Math.min(count - totalEvicted, (int) cache.estimatedSize());
            if (regionEvictCount <= 0) {
                continue;
            }
            
            List<String> keysToEvict = new ArrayList<>();
            List<CacheEntry> entriesToPersist = new ArrayList<>();
            
            // Collect entries to evict
            for (Map.Entry<String, CacheEntry> entry : cache.asMap().entrySet()) {
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
            
            // Persist entries before evicting
            for (CacheEntry entry : entriesToPersist) {
                try {
                    persistenceStore.saveEntry(entry);
                } catch (Exception e) {
                    log.warn("Failed to persist entry '{}' during eviction: {}", 
                            entry.getKey(), e.getMessage());
                }
            }
            
            // Evict from memory
            for (String key : keysToEvict) {
                cache.invalidate(key);
                totalEvicted++;
                
                // Note: We don't decrement region entry count because the entry
                // still exists in persistence. The entry count reflects total entries,
                // not just in-memory entries.
                
                recordStatistic(regionName, "evictions");
            }
            
            if (!keysToEvict.isEmpty()) {
                log.debug("Evicted {} entries from region '{}' to persistence", 
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
        for (Map.Entry<String, Cache<String, CacheEntry>> entry : regionCaches.entrySet()) {
            String regionName = entry.getKey();
            Cache<String, CacheEntry> cache = entry.getValue();
            
            List<String> expiredKeys = new ArrayList<>();
            cache.asMap().forEach((key, cacheEntry) -> {
                if (cacheEntry.isExpired()) {
                    expiredKeys.add(key);
                }
            });
            
            for (String key : expiredKeys) {
                cache.invalidate(key);
                
                // Delete from persistence store
                // Note: entryCount is dynamically calculated from persistence, so no manual decrement needed
                try {
                    persistenceStore.deleteEntry(regionName, key);
                } catch (Exception e) {
                    log.warn("Failed to delete expired entry '{}' from persistence store: {}", key, e.getMessage());
                }
                
                recordStatistic(regionName, KuberConstants.STAT_EXPIRED);
            }
            
            if (!expiredKeys.isEmpty()) {
                log.info("Cleaned up {} expired entries from memory in region '{}'", 
                        expiredKeys.size(), regionName);
            }
        }
    }
    
    // ==================== Graceful Shutdown ====================
    
    @jakarta.annotation.PreDestroy
    public void shutdown() {
        log.info("Shutting down Kuber cache service - persisting all data to {} persistence store...", 
                persistenceStore.getType());
        
        long totalEntries = 0;
        long totalRegions = 0;
        
        try {
            // Persist all regions
            for (CacheRegion region : regions.values()) {
                try {
                    region.setUpdatedAt(Instant.now());
                    persistenceStore.saveRegion(region);
                    totalRegions++;
                } catch (Exception e) {
                    log.error("Failed to persist region '{}': {}", region.getName(), e.getMessage());
                }
            }
            
            // Persist all cache entries from each region
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
                            log.info("Persisted {} entries from region '{}'", validEntries.size(), regionName);
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to persist entries for region '{}': {}", regionName, e.getMessage());
                }
            }
            
            log.info("Shutdown complete - persisted {} regions and {} entries to {} persistence store", 
                    totalRegions, totalEntries, persistenceStore.getType());
            
        } catch (Exception e) {
            log.error("Error during shutdown persistence: {}", e.getMessage(), e);
        }
    }
}
