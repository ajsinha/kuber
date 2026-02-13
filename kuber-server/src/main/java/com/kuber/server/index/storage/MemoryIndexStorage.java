/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index.storage;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * In-memory index storage using ConcurrentHashMap and ConcurrentSkipListMap.
 * 
 * <p>This is the default and fastest storage option. It uses:
 * <ul>
 *   <li>ConcurrentHashMap for O(1) exact lookups</li>
 *   <li>ConcurrentSkipListMap for O(log n) range queries</li>
 * </ul>
 * 
 * <p><b>Characteristics:</b>
 * <ul>
 *   <li>Fastest read/write performance</li>
 *   <li>Uses JVM heap memory</li>
 *   <li>Data lost on restart (requires rebuild)</li>
 *   <li>May cause GC pressure with large indexes</li>
 * </ul>
 * 
 * @version 2.4.0
 * @since 1.9.0
 */
@Slf4j
public class MemoryIndexStorage implements IndexStorageProvider {
    
    // Hash-based index: indexName → (fieldValue → Set<cacheKey>)
    private final ConcurrentHashMap<String, ConcurrentHashMap<Object, Set<String>>> hashIndexes = 
            new ConcurrentHashMap<>();
    
    // Sorted index for range queries: indexName → (fieldValue → Set<cacheKey>)
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Comparable<?>, Set<String>>> sortedIndexes = 
            new ConcurrentHashMap<>();
    
    @Override
    public void addToIndex(String indexName, Object fieldValue, String cacheKey) {
        if (fieldValue == null || cacheKey == null) return;
        
        // Add to hash index
        hashIndexes.computeIfAbsent(indexName, k -> new ConcurrentHashMap<>())
                   .computeIfAbsent(fieldValue, k -> ConcurrentHashMap.newKeySet())
                   .add(cacheKey);
        
        // Add to sorted index if value is Comparable
        if (fieldValue instanceof Comparable) {
            sortedIndexes.computeIfAbsent(indexName, k -> new ConcurrentSkipListMap<>())
                         .computeIfAbsent((Comparable<?>) fieldValue, k -> ConcurrentHashMap.newKeySet())
                         .add(cacheKey);
        }
    }
    
    @Override
    public void removeFromIndex(String indexName, Object fieldValue, String cacheKey) {
        if (fieldValue == null || cacheKey == null) return;
        
        // Remove from hash index
        ConcurrentHashMap<Object, Set<String>> hashIndex = hashIndexes.get(indexName);
        if (hashIndex != null) {
            Set<String> keys = hashIndex.get(fieldValue);
            if (keys != null) {
                keys.remove(cacheKey);
                if (keys.isEmpty()) {
                    hashIndex.remove(fieldValue);
                }
            }
        }
        
        // Remove from sorted index
        if (fieldValue instanceof Comparable) {
            ConcurrentSkipListMap<Comparable<?>, Set<String>> sortedIndex = sortedIndexes.get(indexName);
            if (sortedIndex != null) {
                Set<String> keys = sortedIndex.get(fieldValue);
                if (keys != null) {
                    keys.remove(cacheKey);
                    if (keys.isEmpty()) {
                        sortedIndex.remove(fieldValue);
                    }
                }
            }
        }
    }
    
    @Override
    public Set<String> getExact(String indexName, Object fieldValue) {
        if (fieldValue == null) return Collections.emptySet();
        
        ConcurrentHashMap<Object, Set<String>> index = hashIndexes.get(indexName);
        if (index == null) return Collections.emptySet();
        
        Set<String> keys = index.get(fieldValue);
        return keys != null ? new HashSet<>(keys) : Collections.emptySet();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getRange(String indexName, Object fromValue, Object toValue) {
        ConcurrentSkipListMap<Comparable<?>, Set<String>> index = sortedIndexes.get(indexName);
        if (index == null) return Collections.emptySet();
        
        NavigableMap<Comparable<?>, Set<String>> subMap;
        
        if (fromValue == null && toValue == null) {
            subMap = index;
        } else if (fromValue == null) {
            subMap = index.headMap((Comparable<?>) toValue, true);
        } else if (toValue == null) {
            subMap = index.tailMap((Comparable<?>) fromValue, true);
        } else {
            subMap = index.subMap((Comparable<?>) fromValue, true, (Comparable<?>) toValue, true);
        }
        
        return subMap.values().stream()
                     .flatMap(Set::stream)
                     .collect(Collectors.toSet());
    }
    
    @Override
    public Set<String> getByPrefix(String indexName, String prefix) {
        if (prefix == null || prefix.isEmpty()) return Collections.emptySet();
        
        ConcurrentSkipListMap<Comparable<?>, Set<String>> index = sortedIndexes.get(indexName);
        if (index == null) return Collections.emptySet();
        
        // Get all entries from prefix to prefix + highest char
        String fromKey = prefix;
        String toKey = prefix + Character.MAX_VALUE;
        
        return index.subMap(fromKey, true, toKey, false)
                    .values().stream()
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
    }
    
    @Override
    public void clearIndex(String indexName) {
        hashIndexes.remove(indexName);
        sortedIndexes.remove(indexName);
    }
    
    @Override
    public void clearRegion(String region) {
        // Remove all indexes for this region (indexes are named: region:field:type)
        String prefix = region + ":";
        hashIndexes.keySet().removeIf(k -> k.startsWith(prefix));
        sortedIndexes.keySet().removeIf(k -> k.startsWith(prefix));
    }
    
    @Override
    public long getIndexSize(String indexName) {
        ConcurrentHashMap<Object, Set<String>> index = hashIndexes.get(indexName);
        return index != null ? index.size() : 0;
    }
    
    @Override
    public long getTotalMappings(String indexName) {
        ConcurrentHashMap<Object, Set<String>> index = hashIndexes.get(indexName);
        if (index == null) return 0;
        
        return index.values().stream()
                    .mapToLong(Set::size)
                    .sum();
    }
    
    @Override
    public boolean supportsRangeQueries() {
        return true;
    }
    
    @Override
    public boolean supportsPrefixQueries() {
        return true;
    }
    
    @Override
    public boolean isPersistent() {
        return false;
    }
    
    @Override
    public long getMemoryUsage() {
        // Rough estimate: 64 bytes per entry + key/value sizes
        long estimate = 0;
        for (ConcurrentHashMap<Object, Set<String>> index : hashIndexes.values()) {
            for (Map.Entry<Object, Set<String>> entry : index.entrySet()) {
                estimate += 64; // Entry overhead
                estimate += estimateObjectSize(entry.getKey());
                estimate += entry.getValue().size() * 40; // String references
            }
        }
        return estimate;
    }
    
    private long estimateObjectSize(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof String) return 40 + ((String) obj).length() * 2;
        if (obj instanceof Number) return 24;
        return 40;
    }
    
    @Override
    public StorageType getStorageType() {
        return StorageType.MEMORY;
    }
    
    @Override
    public void initialize() {
        log.info("Initialized in-memory index storage");
    }
    
    @Override
    public void shutdown() {
        hashIndexes.clear();
        sortedIndexes.clear();
        log.info("Shutdown in-memory index storage");
    }
}
