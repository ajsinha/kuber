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
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // In-memory cache per region
    private final Map<String, Cache<String, CacheEntry>> regionCaches = new ConcurrentHashMap<>();
    
    // Region metadata
    private final Map<String, CacheRegion> regions = new ConcurrentHashMap<>();
    
    // Statistics
    private final Map<String, Map<String, Long>> statistics = new ConcurrentHashMap<>();
    
    public CacheService(KuberProperties properties, 
                        PersistenceStore persistenceStore,
                        EventPublisher eventPublisher) {
        this.properties = properties;
        this.persistenceStore = persistenceStore;
        this.eventPublisher = eventPublisher;
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
        
        // Prime cache from persistence store if configured
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
                createCacheForRegion(region.getName());
            }
        } catch (Exception e) {
            log.error("Failed to load regions from persistence store: {}", e.getMessage(), e);
        }
    }
    
    private void createDefaultRegion() {
        CacheRegion defaultRegion = CacheRegion.createDefault();
        regions.put(KuberConstants.DEFAULT_REGION, defaultRegion);
        createCacheForRegion(KuberConstants.DEFAULT_REGION);
        
        try {
            persistenceStore.saveRegion(defaultRegion);
        } catch (Exception e) {
            log.warn("Failed to persist default region: {}", e.getMessage());
        }
    }
    
    private void createCacheForRegion(String regionName) {
        Cache<String, CacheEntry> cache = Caffeine.newBuilder()
                .maximumSize(properties.getCache().getMaxMemoryEntries())
                .expireAfterWrite(24, TimeUnit.HOURS)
                .removalListener((key, value, cause) -> {
                    if (cause == RemovalCause.SIZE || cause == RemovalCause.EXPIRED) {
                        // Decrement entry count when evicted due to size limit or Caffeine's TTL
                        CacheRegion region = regions.get(regionName);
                        if (region != null) {
                            region.decrementEntryCount();
                        }
                        recordStatistic(regionName, KuberConstants.STAT_EVICTED);
                        log.debug("Entry '{}' evicted from region '{}' due to {}", key, regionName, cause);
                    }
                })
                .recordStats()
                .build();
        
        regionCaches.put(regionName, cache);
        statistics.put(regionName, new ConcurrentHashMap<>());
    }
    
    private void primeCache() {
        log.info("Priming cache from {} persistence store...", persistenceStore.getType());
        
        for (String regionName : regions.keySet()) {
            try {
                List<CacheEntry> entries = persistenceStore.loadEntries(regionName, 
                        properties.getCache().getMaxMemoryEntries());
                Cache<String, CacheEntry> cache = regionCaches.get(regionName);
                
                for (CacheEntry entry : entries) {
                    if (!entry.isExpired()) {
                        cache.put(entry.getKey(), entry);
                    }
                }
                
                log.info("Primed {} entries for region '{}'", entries.size(), regionName);
            } catch (Exception e) {
                log.warn("Failed to prime cache for region '{}': {}", regionName, e.getMessage());
            }
        }
    }
    
    // ==================== Region Operations ====================
    
    public CacheRegion createRegion(String name, String description) {
        checkWriteAccess();
        
        if (regions.containsKey(name)) {
            throw RegionException.alreadyExists(name);
        }
        
        // Generate collection name
        String collectionName = "kuber_" + name.toLowerCase().replaceAll("[^a-z0-9_]", "_");
        
        CacheRegion region = CacheRegion.builder()
                .name(name)
                .description(description)
                .collectionName(collectionName)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        
        regions.put(name, region);
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
        
        log.info("Created region: {}", name);
        return region;
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
        return Collections.unmodifiableCollection(regions.values());
    }
    
    public CacheRegion getRegion(String name) {
        return regions.get(name);
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
        CacheEntry removed = cache.getIfPresent(key);
        
        if (removed != null) {
            cache.invalidate(key);
            persistenceStore.deleteEntry(region, key);
            
            CacheRegion regionObj = regions.get(region);
            if (regionObj != null) {
                regionObj.decrementEntryCount();
            }
            
            recordStatistic(region, KuberConstants.STAT_DELETES);
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
        
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        Pattern regex = globToRegex(pattern);
        
        return cache.asMap().keySet().stream()
                .filter(key -> regex.matcher(key).matches())
                .collect(Collectors.toSet());
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
        
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        List<Map<String, Object>> results = new ArrayList<>();
        
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
                
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("key", key);
                result.put("value", cacheEntry.getStringValue());
                result.put("type", cacheEntry.getValueType().name().toLowerCase());
                result.put("ttl", cacheEntry.getRemainingTtl());
                
                // Include JSON value if applicable
                if (cacheEntry.getValueType() == CacheEntry.ValueType.JSON && cacheEntry.getJsonValue() != null) {
                    result.put("jsonValue", cacheEntry.getJsonValue());
                }
                
                results.add(result);
                recordStatistic(region, KuberConstants.STAT_HITS);
            }
        }
        
        return results;
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
        ensureRegionExists(region);
        
        List<JsonUtils.QueryCondition> conditions = JsonUtils.parseQuery(query);
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        
        return cache.asMap().values().stream()
                .filter(entry -> entry.getValueType() == CacheEntry.ValueType.JSON)
                .filter(entry -> entry.getJsonValue() != null)
                .filter(entry -> !entry.isExpired())
                .filter(entry -> JsonUtils.matchesAllQueries(entry.getJsonValue(), conditions))
                .collect(Collectors.toList());
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
                
                // Decrement region entry count
                CacheRegion regionObj = regions.get(region);
                if (regionObj != null) {
                    regionObj.decrementEntryCount();
                }
                
                // Delete from persistence store
                try {
                    persistenceStore.deleteEntry(region, key);
                } catch (Exception e) {
                    log.warn("Failed to delete expired entry '{}' from persistence store: {}", key, e.getMessage());
                }
                
                recordStatistic(region, KuberConstants.STAT_EXPIRED);
                return null;
            }
            
            entry.recordAccess();
            regions.get(region).recordHit();
            recordStatistic(region, KuberConstants.STAT_HITS);
        } else {
            regions.get(region).recordMiss();
            recordStatistic(region, KuberConstants.STAT_MISSES);
        }
        
        return entry;
    }
    
    private void putEntry(String region, String key, CacheEntry entry) {
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        
        boolean isNew = cache.getIfPresent(key) == null;
        cache.put(key, entry);
        
        if (isNew) {
            CacheRegion regionObj = regions.get(region);
            if (regionObj != null) {
                regionObj.incrementEntryCount();
            }
        }
        
        recordStatistic(region, KuberConstants.STAT_SETS);
        
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
        info.put("version", "1.0.0");
        info.put("regionCount", regions.size());
        info.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        info.put("maxMemoryEntries", properties.getCache().getMaxMemoryEntries());
        info.put("persistentMode", properties.getCache().isPersistentMode());
        
        long totalEntries = regionCaches.values().stream()
                .mapToLong(Cache::estimatedSize)
                .sum();
        info.put("totalEntries", totalEntries);
        
        return info;
    }
    
    public long dbSize(String region) {
        if (region == null || region.isEmpty()) {
            return regionCaches.values().stream()
                    .mapToLong(Cache::estimatedSize)
                    .sum();
        }
        
        Cache<String, CacheEntry> cache = regionCaches.get(region);
        return cache != null ? cache.estimatedSize() : 0;
    }
    
    // ==================== TTL Cleanup ====================
    
    @Scheduled(fixedRateString = "${kuber.cache.ttl-cleanup-interval-seconds:60}000")
    public void cleanupExpiredEntries() {
        for (Map.Entry<String, Cache<String, CacheEntry>> entry : regionCaches.entrySet()) {
            String regionName = entry.getKey();
            Cache<String, CacheEntry> cache = entry.getValue();
            CacheRegion region = regions.get(regionName);
            
            List<String> expiredKeys = new ArrayList<>();
            cache.asMap().forEach((key, cacheEntry) -> {
                if (cacheEntry.isExpired()) {
                    expiredKeys.add(key);
                }
            });
            
            for (String key : expiredKeys) {
                cache.invalidate(key);
                
                // Decrement the region entry count
                if (region != null) {
                    region.decrementEntryCount();
                }
                
                // Delete from persistence store
                try {
                    persistenceStore.deleteEntry(regionName, key);
                } catch (Exception e) {
                    log.warn("Failed to delete expired entry '{}' from persistence store: {}", key, e.getMessage());
                }
                
                recordStatistic(regionName, KuberConstants.STAT_EXPIRED);
            }
            
            if (!expiredKeys.isEmpty()) {
                log.info("Cleaned up {} expired entries from region '{}', new count: {}", 
                        expiredKeys.size(), regionName, region != null ? region.getEntryCount() : "N/A");
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
