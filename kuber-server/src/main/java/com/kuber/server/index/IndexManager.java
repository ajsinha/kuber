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
package com.kuber.server.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central manager for secondary indexes in Kuber.
 * 
 * <p>The IndexManager is responsible for:
 * <ul>
 *   <li>Creating and managing secondary indexes for regions</li>
 *   <li>Automatically updating indexes on document changes</li>
 *   <li>Providing fast index lookups for queries</li>
 *   <li>Persisting indexes to RocksDB (hybrid mode)</li>
 *   <li>Rebuilding indexes on startup or on demand</li>
 * </ul>
 * 
 * <h3>Storage Modes:</h3>
 * <ul>
 *   <li><strong>HEAP</strong>: On-heap storage, fastest queries, GC pressure</li>
 *   <li><strong>OFFHEAP</strong>: Off-heap storage, slightly slower but zero GC pressure</li>
 * </ul>
 * 
 * <h3>Performance:</h3>
 * <ul>
 *   <li>Hash index lookup: O(1)</li>
 *   <li>BTree index lookup: O(log n)</li>
 *   <li>Index intersection: O(min(m, n))</li>
 *   <li>Without index (full scan): O(n)</li>
 * </ul>
 * 
 * @version 1.8.2
 * @since 1.8.2
 */
@Service
@Slf4j
public class IndexManager implements IndexConfiguration.IndexConfigurationListener {
    
    private final IndexConfiguration indexConfiguration;
    private final CacheService cacheService;
    private final KuberProperties properties;
    
    // Region -> Field -> SecondaryIndex
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, SecondaryIndex>> regionIndexes;
    
    // Thread pool for index operations
    private ExecutorService indexExecutor;
    
    // Statistics
    private final AtomicLong totalIndexLookups = new AtomicLong(0);
    private final AtomicLong totalIndexUpdates = new AtomicLong(0);
    private final AtomicLong totalRebuildTime = new AtomicLong(0);
    private final AtomicLong indexHits = new AtomicLong(0);
    private final AtomicLong indexMisses = new AtomicLong(0);
    
    // Persistence tracking
    private volatile boolean persistenceDirty = false;
    private volatile Instant lastPersistenceSync = Instant.now();
    
    public IndexManager(IndexConfiguration indexConfiguration, @Lazy CacheService cacheService,
                        KuberProperties properties) {
        this.indexConfiguration = indexConfiguration;
        this.cacheService = cacheService;
        this.properties = properties;
        this.regionIndexes = new ConcurrentHashMap<>();
    }
    
    @PostConstruct
    public void initialize() {
        if (!indexConfiguration.isIndexingEnabled()) {
            log.info("Secondary indexing is DISABLED");
            return;
        }
        
        // Create thread pool for index operations
        int threadCount = indexConfiguration.getIndexingSettings().getRebuildThreads();
        indexExecutor = Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("kuber-index-" + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        
        // Register for configuration changes
        indexConfiguration.addListener(this);
        
        // Create indexes from configuration
        initializeIndexesFromConfig();
        
        // Rebuild indexes if needed
        if (indexConfiguration.getIndexingSettings().isRebuildOnStartup()) {
            rebuildAllIndexes();
        }
        
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║              INDEX MANAGER INITIALIZED                       ║");
        log.info("╠══════════════════════════════════════════════════════════════╣");
        log.info("║  Status:       ENABLED                                       ║");
        log.info("║  Storage:      {}                                         ║", 
            String.format("%-8s", indexConfiguration.getStorageMode()));
        log.info("║  Regions:      {}                                            ║", 
            String.format("%-4d", regionIndexes.size()));
        log.info("║  Indexes:      {}                                            ║", 
            String.format("%-4d", getTotalIndexCount()));
        log.info("╚══════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Initialize indexes from configuration.
     */
    private void initializeIndexesFromConfig() {
        for (Map.Entry<String, IndexConfiguration.RegionIndexConfig> entry : 
                indexConfiguration.getRegionConfigs().entrySet()) {
            
            String region = entry.getKey();
            IndexConfiguration.RegionIndexConfig config = entry.getValue();
            
            for (IndexDefinition indexDef : config.getIndexes()) {
                createIndex(region, indexDef);
            }
        }
    }
    
    // ==================== Index Creation ====================
    
    /**
     * Create a new secondary index.
     * 
     * @param region Region name
     * @param indexDef Index definition
     * @return true if index was created, false if already exists
     */
    public boolean createIndex(String region, IndexDefinition indexDef) {
        if (region == null || indexDef == null || indexDef.getField() == null) {
            return false;
        }
        
        indexDef.setRegion(region);
        if (indexDef.getCreatedAt() == null) {
            indexDef.setCreatedAt(Instant.now());
        }
        
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = 
            regionIndexes.computeIfAbsent(region, k -> new ConcurrentHashMap<>());
        
        String field = indexDef.getField();
        
        // Check if already exists
        if (fieldIndexes.containsKey(field)) {
            log.debug("Index already exists: region={}, field={}", region, field);
            return false;
        }
        
        // Check max indexes limit
        int maxIndexes = indexConfiguration.getIndexingSettings().getMaxIndexesPerRegion();
        if (fieldIndexes.size() >= maxIndexes) {
            log.warn("Maximum indexes ({}) reached for region: {}", maxIndexes, region);
            return false;
        }
        
        // Create the appropriate index type
        SecondaryIndex index = createIndexInstance(indexDef);
        fieldIndexes.put(field, index);
        
        // Add to configuration (for runtime-created indexes)
        indexConfiguration.addIndex(region, indexDef);
        
        log.info("Created {} index: region={}, field={}", indexDef.getType(), region, field);
        return true;
    }
    
    /**
     * Create a new index with specified parameters.
     */
    public boolean createIndex(String region, String field, IndexType type, String description) {
        IndexDefinition indexDef = IndexDefinition.builder()
            .region(region)
            .field(field)
            .type(type)
            .description(description)
            .createdAt(Instant.now())
            .build();
        
        return createIndex(region, indexDef);
    }
    
    /**
     * Create index instance based on type.
     */
    private SecondaryIndex createIndexInstance(IndexDefinition indexDef) {
        boolean useOffHeap = shouldUseOffHeap(indexDef.getType());
        long initialSize = properties.getIndexing().getOffheapInitialSize();
        long maxSize = properties.getIndexing().getOffheapMaxSize();
        
        return switch (indexDef.getType()) {
            case BTREE -> new BTreeIndex(indexDef); // TODO: OffHeapBTreeIndex
            case HASH -> useOffHeap 
                ? new OffHeapHashIndex(indexDef, initialSize, maxSize) 
                : new HashIndex(indexDef);
            case TRIGRAM -> new TrigramIndex(indexDef); // TODO: OffHeapTrigramIndex
            case PREFIX -> new PrefixIndex(indexDef); // TODO: OffHeapPrefixIndex
        };
    }
    
    /**
     * Determine if off-heap storage should be used for an index type.
     */
    private boolean shouldUseOffHeap(IndexType type) {
        KuberProperties.Indexing indexing = properties.getIndexing();
        String defaultStorage = indexing.getDefaultStorage();
        
        String typeStorage = switch (type) {
            case HASH -> indexing.getHashStorage();
            case BTREE -> indexing.getBtreeStorage();
            case TRIGRAM -> indexing.getTrigramStorage();
            case PREFIX -> indexing.getPrefixStorage();
        };
        
        // Check type-specific setting first
        if (typeStorage != null && !typeStorage.equalsIgnoreCase("DEFAULT")) {
            return typeStorage.equalsIgnoreCase("OFFHEAP");
        }
        
        // Fall back to default
        return defaultStorage != null && defaultStorage.equalsIgnoreCase("OFFHEAP");
    }
    
    // ==================== Index Removal ====================
    
    /**
     * Drop an index from a region.
     * 
     * @param region Region name
     * @param field Field name
     * @return true if index was dropped
     */
    public boolean dropIndex(String region, String field) {
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        if (fieldIndexes == null) {
            return false;
        }
        
        SecondaryIndex removed = fieldIndexes.remove(field);
        if (removed != null) {
            removed.shutdown(); // Properly release resources (especially off-heap)
            indexConfiguration.removeIndex(region, field);
            log.info("Dropped index: region={}, field={}, storage={}", 
                region, field, removed.isOffHeap() ? "OFFHEAP" : "HEAP");
            return true;
        }
        return false;
    }
    
    /**
     * Drop all indexes for a region.
     */
    public void dropAllIndexes(String region) {
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.remove(region);
        if (fieldIndexes != null) {
            for (SecondaryIndex index : fieldIndexes.values()) {
                index.shutdown(); // Properly release resources
            }
            log.info("Dropped all indexes for region: {}", region);
        }
    }
    
    // ==================== Index Updates ====================
    
    /**
     * Update indexes when a document is added.
     * Called by CacheService on JSET/SET operations.
     * 
     * @param region Region name
     * @param key Document key
     * @param document JSON document
     */
    public void onDocumentAdded(String region, String key, JsonNode document) {
        if (!indexConfiguration.isIndexingEnabled() || document == null) {
            return;
        }
        
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        if (fieldIndexes == null || fieldIndexes.isEmpty()) {
            return;
        }
        
        for (Map.Entry<String, SecondaryIndex> entry : fieldIndexes.entrySet()) {
            String field = entry.getKey();
            SecondaryIndex index = entry.getValue();
            
            Object fieldValue = extractFieldValue(document, field);
            if (fieldValue != null) {
                index.add(fieldValue, key);
            }
        }
        
        totalIndexUpdates.incrementAndGet();
        persistenceDirty = true;
    }
    
    /**
     * Update indexes when a document is updated.
     * 
     * @param region Region name
     * @param key Document key
     * @param oldDocument Previous document (null if not available)
     * @param newDocument New document
     */
    public void onDocumentUpdated(String region, String key, JsonNode oldDocument, JsonNode newDocument) {
        if (!indexConfiguration.isIndexingEnabled()) {
            return;
        }
        
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        if (fieldIndexes == null || fieldIndexes.isEmpty()) {
            return;
        }
        
        for (Map.Entry<String, SecondaryIndex> entry : fieldIndexes.entrySet()) {
            String field = entry.getKey();
            SecondaryIndex index = entry.getValue();
            
            Object oldValue = oldDocument != null ? extractFieldValue(oldDocument, field) : null;
            Object newValue = newDocument != null ? extractFieldValue(newDocument, field) : null;
            
            if (oldValue != null && newValue != null) {
                index.update(oldValue, newValue, key);
            } else if (oldValue != null) {
                index.remove(oldValue, key);
            } else if (newValue != null) {
                index.add(newValue, key);
            }
        }
        
        totalIndexUpdates.incrementAndGet();
        persistenceDirty = true;
    }
    
    /**
     * Update indexes when a document is deleted.
     * 
     * @param region Region name
     * @param key Document key
     * @param document The deleted document
     */
    public void onDocumentDeleted(String region, String key, JsonNode document) {
        if (!indexConfiguration.isIndexingEnabled()) {
            return;
        }
        
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        if (fieldIndexes == null || fieldIndexes.isEmpty()) {
            return;
        }
        
        if (document != null) {
            for (Map.Entry<String, SecondaryIndex> entry : fieldIndexes.entrySet()) {
                String field = entry.getKey();
                SecondaryIndex index = entry.getValue();
                
                Object fieldValue = extractFieldValue(document, field);
                if (fieldValue != null) {
                    index.remove(fieldValue, key);
                }
            }
        }
        
        totalIndexUpdates.incrementAndGet();
        persistenceDirty = true;
    }
    
    // ==================== Index Queries ====================
    
    /**
     * Find document keys using index for equality query.
     * 
     * @param region Region name
     * @param field Field name
     * @param value Value to match
     * @return Set of matching document keys, or null if no index
     */
    public Set<String> findEquals(String region, String field, Object value) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null) {
            indexMisses.incrementAndGet();
            return null; // No index - caller should fall back to scan
        }
        
        totalIndexLookups.incrementAndGet();
        indexHits.incrementAndGet();
        return index.findEquals(value);
    }
    
    /**
     * Find document keys using index for IN query.
     * 
     * @param region Region name
     * @param field Field name
     * @param values Values to match (OR)
     * @return Set of matching document keys, or null if no index
     */
    public Set<String> findIn(String region, String field, Set<Object> values) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null) {
            indexMisses.incrementAndGet();
            return null;
        }
        
        totalIndexLookups.incrementAndGet();
        indexHits.incrementAndGet();
        return index.findIn(values);
    }
    
    /**
     * Find document keys using index for range query.
     * 
     * @param region Region name
     * @param field Field name
     * @param from Lower bound (null for no lower bound)
     * @param to Upper bound (null for no upper bound)
     * @param fromInclusive Include lower bound
     * @param toInclusive Include upper bound
     * @return Set of matching document keys, or null if no index
     */
    public Set<String> findRange(String region, String field, Object from, Object to, 
                                  boolean fromInclusive, boolean toInclusive) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null) {
            indexMisses.incrementAndGet();
            return null;
        }
        
        if (!index.supportsRangeQueries()) {
            log.debug("Index {} does not support range queries", field);
            return null;
        }
        
        totalIndexLookups.incrementAndGet();
        indexHits.incrementAndGet();
        return index.findRange(from, to, fromInclusive, toInclusive);
    }
    
    /**
     * Find document keys for greater than query.
     */
    public Set<String> findGreaterThan(String region, String field, Object value, boolean inclusive) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null || !index.supportsRangeQueries()) {
            return null;
        }
        
        totalIndexLookups.incrementAndGet();
        indexHits.incrementAndGet();
        return index.findGreaterThan(value, inclusive);
    }
    
    /**
     * Find document keys for less than query.
     */
    public Set<String> findLessThan(String region, String field, Object value, boolean inclusive) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null || !index.supportsRangeQueries()) {
            return null;
        }
        
        totalIndexLookups.incrementAndGet();
        indexHits.incrementAndGet();
        return index.findLessThan(value, inclusive);
    }
    
    /**
     * Find document keys using regex pattern.
     * Most efficient with TRIGRAM indexes.
     * 
     * @param region Region name
     * @param field Field name
     * @param pattern Compiled regex pattern
     * @return Set of candidate document keys, or null if no suitable index
     */
    public Set<String> findRegex(String region, String field, java.util.regex.Pattern pattern) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null) {
            indexMisses.incrementAndGet();
            return null;
        }
        
        totalIndexLookups.incrementAndGet();
        
        Set<String> result = index.findRegex(pattern);
        if (result != null) {
            indexHits.incrementAndGet();
        } else {
            // Index couldn't help - return null to indicate full scan needed
            indexMisses.incrementAndGet();
        }
        
        return result;
    }
    
    /**
     * Find document keys where field value starts with prefix.
     * Most efficient with PREFIX indexes, also supported by TRIGRAM.
     * 
     * @param region Region name
     * @param field Field name
     * @param prefix The prefix to search for
     * @return Set of matching document keys, or null if no suitable index
     */
    public Set<String> findPrefix(String region, String field, String prefix) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null) {
            indexMisses.incrementAndGet();
            return null;
        }
        
        totalIndexLookups.incrementAndGet();
        
        Set<String> result = index.findPrefix(prefix);
        if (result != null) {
            indexHits.incrementAndGet();
        } else {
            indexMisses.incrementAndGet();
        }
        
        return result;
    }
    
    /**
     * Find document keys where field value contains substring.
     * Most efficient with TRIGRAM indexes.
     * 
     * @param region Region name
     * @param field Field name
     * @param substring The substring to search for
     * @return Set of matching document keys, or null if no suitable index
     */
    public Set<String> findContains(String region, String field, String substring) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null) {
            indexMisses.incrementAndGet();
            return null;
        }
        
        totalIndexLookups.incrementAndGet();
        
        Set<String> result = index.findContains(substring);
        if (result != null) {
            indexHits.incrementAndGet();
        } else {
            indexMisses.incrementAndGet();
        }
        
        return result;
    }
    
    /**
     * Check if an index supports regex queries efficiently.
     */
    public boolean supportsRegex(String region, String field) {
        SecondaryIndex index = getIndex(region, field);
        return index != null && index.supportsRegexQueries();
    }
    
    /**
     * Check if an index supports prefix queries efficiently.
     */
    public boolean supportsPrefix(String region, String field) {
        SecondaryIndex index = getIndex(region, field);
        return index != null && index.supportsPrefixQueries();
    }
    
    /**
     * Check if an index exists for a field.
     */
    public boolean hasIndex(String region, String field) {
        return getIndex(region, field) != null;
    }
    
    /**
     * Get an index for a region/field.
     */
    public SecondaryIndex getIndex(String region, String field) {
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        return fieldIndexes != null ? fieldIndexes.get(field) : null;
    }
    
    /**
     * Get all indexes for a region.
     */
    public Map<String, SecondaryIndex> getIndexes(String region) {
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        return fieldIndexes != null ? Collections.unmodifiableMap(fieldIndexes) : Collections.emptyMap();
    }
    
    // ==================== Index Rebuild ====================
    
    /**
     * Rebuild all indexes from data.
     */
    public void rebuildAllIndexes() {
        log.info("Starting rebuild of all indexes...");
        long startTime = System.currentTimeMillis();
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (String region : regionIndexes.keySet()) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(
                () -> rebuildRegionIndexes(region), indexExecutor);
            futures.add(future);
        }
        
        // Wait for all rebuilds
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        long elapsed = System.currentTimeMillis() - startTime;
        totalRebuildTime.addAndGet(elapsed);
        
        log.info("All indexes rebuilt in {}ms", elapsed);
    }
    
    /**
     * Rebuild all indexes for a specific region.
     * 
     * @param region Region name
     */
    public void rebuildRegionIndexes(String region) {
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        if (fieldIndexes == null || fieldIndexes.isEmpty()) {
            return;
        }
        
        log.info("Rebuilding indexes for region: {}", region);
        long startTime = System.currentTimeMillis();
        
        // Clear existing entries
        for (SecondaryIndex index : fieldIndexes.values()) {
            index.clear();
        }
        
        // Get all keys in region
        Set<String> keys = cacheService.keys(region, "*");
        if (keys == null || keys.isEmpty()) {
            log.debug("No documents in region {} for indexing", region);
            return;
        }
        
        AtomicInteger indexed = new AtomicInteger(0);
        
        // Index all documents
        for (String key : keys) {
            try {
                JsonNode document = cacheService.jsonGet(region, key, "$");
                if (document != null) {
                    for (Map.Entry<String, SecondaryIndex> entry : fieldIndexes.entrySet()) {
                        String field = entry.getKey();
                        SecondaryIndex index = entry.getValue();
                        
                        Object fieldValue = extractFieldValue(document, field);
                        if (fieldValue != null) {
                            index.add(fieldValue, key);
                        }
                    }
                    indexed.incrementAndGet();
                }
            } catch (Exception e) {
                log.trace("Error indexing key {}: {}", key, e.getMessage());
            }
        }
        
        // Update rebuild time
        for (SecondaryIndex index : fieldIndexes.values()) {
            index.getDefinition().setLastRebuiltAt(Instant.now());
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Rebuilt {} indexes for region {} ({} documents) in {}ms", 
            fieldIndexes.size(), region, indexed.get(), elapsed);
    }
    
    /**
     * Rebuild a specific index.
     * 
     * @param region Region name
     * @param field Field name
     */
    public void rebuildIndex(String region, String field) {
        SecondaryIndex index = getIndex(region, field);
        if (index == null) {
            return;
        }
        
        log.info("Rebuilding index: region={}, field={}", region, field);
        long startTime = System.currentTimeMillis();
        
        index.clear();
        
        Set<String> keys = cacheService.keys(region, "*");
        log.debug("Found {} keys in region {}", keys != null ? keys.size() : 0, region);
        
        int indexed = 0;
        int noJson = 0;
        int noField = 0;
        
        if (keys != null) {
            for (String key : keys) {
                try {
                    JsonNode document = cacheService.jsonGet(region, key, "$");
                    if (document != null) {
                        Object fieldValue = extractFieldValue(document, field);
                        if (fieldValue != null) {
                            index.add(fieldValue, key);
                            indexed++;
                        } else {
                            noField++;
                        }
                    } else {
                        noJson++;
                    }
                } catch (Exception e) {
                    log.trace("Error indexing key {}: {}", key, e.getMessage());
                }
            }
        }
        
        if (noJson > 0 || noField > 0) {
            log.warn("Index rebuild stats for {}.{}: indexed={}, noJsonValue={}, noField={}", 
                region, field, indexed, noJson, noField);
        }
        
        index.getDefinition().setLastRebuiltAt(Instant.now());
        
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Rebuilt index {}.{} ({} entries) in {}ms", 
            region, field, index.size(), elapsed);
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Extract field value from JSON document, supporting dot notation.
     */
    private Object extractFieldValue(JsonNode document, String field) {
        if (document == null || field == null) {
            return null;
        }
        
        // Handle composite fields
        if (field.contains(",")) {
            StringBuilder compositeValue = new StringBuilder();
            for (String f : field.split(",")) {
                Object v = extractSingleFieldValue(document, f.trim());
                if (v == null) return null; // All parts required
                if (compositeValue.length() > 0) compositeValue.append("|");
                compositeValue.append(v);
            }
            return compositeValue.toString();
        }
        
        return extractSingleFieldValue(document, field);
    }
    
    private Object extractSingleFieldValue(JsonNode document, String field) {
        String[] parts = field.split("\\.");
        JsonNode current = document;
        
        for (String part : parts) {
            if (current == null || !current.has(part)) {
                return null;
            }
            current = current.get(part);
        }
        
        if (current == null || current.isNull() || current.isMissingNode()) {
            return null;
        }
        
        // Convert to appropriate type
        if (current.isNumber()) {
            return current.numberValue();
        } else if (current.isBoolean()) {
            return current.booleanValue();
        } else {
            return current.asText();
        }
    }
    
    // ==================== Statistics ====================
    
    /**
     * Get statistics for all indexes.
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        
        stats.put("enabled", indexConfiguration.isIndexingEnabled());
        stats.put("storageMode", indexConfiguration.getStorageMode());
        stats.put("totalRegions", regionIndexes.size());
        stats.put("totalIndexes", getTotalIndexCount());
        stats.put("totalIndexLookups", totalIndexLookups.get());
        stats.put("totalIndexUpdates", totalIndexUpdates.get());
        stats.put("indexHits", indexHits.get());
        stats.put("indexMisses", indexMisses.get());
        stats.put("totalRebuildTimeMs", totalRebuildTime.get());
        
        long total = indexHits.get() + indexMisses.get();
        if (total > 0) {
            stats.put("hitRate", String.format("%.1f%%", (indexHits.get() * 100.0) / total));
        }
        
        // Memory usage
        long totalMemory = 0;
        for (ConcurrentHashMap<String, SecondaryIndex> fieldIndexes : regionIndexes.values()) {
            for (SecondaryIndex index : fieldIndexes.values()) {
                totalMemory += index.memoryUsageBytes();
            }
        }
        stats.put("totalMemoryBytes", totalMemory);
        stats.put("totalMemoryMB", String.format("%.2f", totalMemory / (1024.0 * 1024.0)));
        
        return stats;
    }
    
    /**
     * Get statistics for a specific region.
     */
    public Map<String, Object> getRegionStatistics(String region) {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("region", region);
        
        ConcurrentHashMap<String, SecondaryIndex> fieldIndexes = regionIndexes.get(region);
        if (fieldIndexes == null) {
            stats.put("indexCount", 0);
            stats.put("indexes", Collections.emptyList());
            return stats;
        }
        
        stats.put("indexCount", fieldIndexes.size());
        
        List<Map<String, Object>> indexStats = new ArrayList<>();
        long totalMemory = 0;
        long totalOffHeap = 0;
        long totalEntries = 0;
        
        for (SecondaryIndex index : fieldIndexes.values()) {
            Map<String, Object> idxStats = index.getStatistics();
            idxStats.put("isOffHeap", index.isOffHeap());
            idxStats.put("offHeapBytes", index.getOffHeapBytesUsed());
            indexStats.add(idxStats);
            totalMemory += index.memoryUsageBytes();
            totalOffHeap += index.getOffHeapBytesUsed();
            totalEntries += index.size();
        }
        
        stats.put("indexes", indexStats);
        stats.put("totalEntries", totalEntries);
        stats.put("totalMemoryBytes", totalMemory);
        stats.put("memoryMB", String.format("%.2f MB", totalMemory / 1048576.0));
        stats.put("totalOffHeapBytes", totalOffHeap);
        stats.put("offHeapMB", String.format("%.2f MB", totalOffHeap / 1048576.0));
        
        return stats;
    }
    
    /**
     * List all indexes.
     */
    public List<Map<String, Object>> listAllIndexes() {
        List<Map<String, Object>> result = new ArrayList<>();
        
        for (Map.Entry<String, ConcurrentHashMap<String, SecondaryIndex>> regionEntry : 
                regionIndexes.entrySet()) {
            String region = regionEntry.getKey();
            
            for (SecondaryIndex index : regionEntry.getValue().values()) {
                Map<String, Object> indexInfo = new LinkedHashMap<>();
                indexInfo.put("region", region);
                indexInfo.putAll(index.getStatistics());
                // Add storage type info
                indexInfo.put("isOffHeap", index.isOffHeap());
                indexInfo.put("offHeapBytes", index.getOffHeapBytesUsed());
                // Format memory for display
                long memBytes = index.memoryUsageBytes();
                indexInfo.put("memoryMB", String.format("%.2f MB", memBytes / 1048576.0));
                result.add(indexInfo);
            }
        }
        
        return result;
    }
    
    /**
     * Get list of regions that have indexes defined.
     * @return Set of region names with indexes
     */
    public Set<String> getIndexedRegions() {
        return new HashSet<>(regionIndexes.keySet());
    }
    
    private int getTotalIndexCount() {
        int count = 0;
        for (ConcurrentHashMap<String, SecondaryIndex> fieldIndexes : regionIndexes.values()) {
            count += fieldIndexes.size();
        }
        return count;
    }
    
    // ==================== Configuration Listener ====================
    
    @Override
    public void onConfigurationChanged(IndexConfiguration config) {
        log.info("Index configuration changed, updating indexes...");
        initializeIndexesFromConfig();
    }
    
    // ==================== Scheduled Tasks ====================
    
    /**
     * Periodic persistence sync (for hybrid mode).
     */
    @Scheduled(fixedDelayString = "${kuber.indexing.persistence.sync-interval-seconds:30}000")
    public void syncToPersistence() {
        if (!indexConfiguration.isIndexingEnabled()) {
            return;
        }
        
        if (!persistenceDirty) {
            return;
        }
        
        String storageMode = indexConfiguration.getStorageMode();
        if ("memory".equals(storageMode)) {
            return; // No persistence needed
        }
        
        // TODO: Implement persistence to RocksDB column family
        persistenceDirty = false;
        lastPersistenceSync = Instant.now();
    }
    
    @PreDestroy
    public void shutdown() {
        indexConfiguration.removeListener(this);
        indexConfiguration.stopWatcher();
        
        if (indexExecutor != null && !indexExecutor.isShutdown()) {
            indexExecutor.shutdown();
            try {
                if (!indexExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    indexExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                indexExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        log.info("IndexManager shut down. Stats: {} lookups, {} updates, {}% hit rate",
            totalIndexLookups.get(), totalIndexUpdates.get(),
            (indexHits.get() + indexMisses.get()) > 0 
                ? String.format("%.1f", (indexHits.get() * 100.0) / (indexHits.get() + indexMisses.get()))
                : "N/A");
    }
}
