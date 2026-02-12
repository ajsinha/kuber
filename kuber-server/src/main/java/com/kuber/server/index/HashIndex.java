/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hash-based secondary index for equality queries.
 * 
 * <p>Provides O(1) lookup for equality queries like:
 * <ul>
 *   <li>status = "active"</li>
 *   <li>city = "NYC"</li>
 *   <li>category IN ["electronics", "clothing"]</li>
 * </ul>
 * 
 * <p>Thread-safe implementation using ConcurrentHashMap.
 * 
 * @version 2.3.0
 * @since 1.9.0
 */
@Slf4j
public class HashIndex implements SecondaryIndex {
    
    private final IndexDefinition definition;
    
    // Map: fieldValue -> Set of document keys
    private final ConcurrentHashMap<Object, Set<String>> index;
    
    // Statistics
    private final AtomicLong totalEntries = new AtomicLong(0);
    
    public HashIndex(IndexDefinition definition) {
        this.definition = definition;
        this.index = new ConcurrentHashMap<>();
        log.debug("Created HashIndex for field: {}", definition.getField());
    }
    
    @Override
    public IndexDefinition getDefinition() {
        return definition;
    }
    
    @Override
    public void add(Object fieldValue, String documentKey) {
        if (fieldValue == null || documentKey == null) {
            return;
        }
        
        Object normalizedValue = normalizeValue(fieldValue);
        index.computeIfAbsent(normalizedValue, k -> ConcurrentHashMap.newKeySet())
             .add(documentKey);
        totalEntries.incrementAndGet();
    }
    
    @Override
    public void remove(Object fieldValue, String documentKey) {
        if (fieldValue == null || documentKey == null) {
            return;
        }
        
        Object normalizedValue = normalizeValue(fieldValue);
        Set<String> keys = index.get(normalizedValue);
        if (keys != null) {
            if (keys.remove(documentKey)) {
                totalEntries.decrementAndGet();
            }
            // Remove empty sets
            if (keys.isEmpty()) {
                index.remove(normalizedValue, keys);
            }
        }
    }
    
    @Override
    public void update(Object oldValue, Object newValue, String documentKey) {
        if (documentKey == null) {
            return;
        }
        
        // If values are the same, nothing to do
        if (Objects.equals(normalizeValue(oldValue), normalizeValue(newValue))) {
            return;
        }
        
        remove(oldValue, documentKey);
        add(newValue, documentKey);
    }
    
    @Override
    public Set<String> findEquals(Object fieldValue) {
        if (fieldValue == null) {
            return Collections.emptySet();
        }
        
        Object normalizedValue = normalizeValue(fieldValue);
        Set<String> result = index.get(normalizedValue);
        // v2.2.0: Return unmodifiable view instead of defensive copy to reduce GC
        return result != null ? Collections.unmodifiableSet(result) : Collections.emptySet();
    }
    
    @Override
    public Set<String> findIn(Set<Object> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        
        // For IN queries we still need to create a new set for union
        Set<String> result = new HashSet<>();
        for (Object value : values) {
            Set<String> matches = index.get(normalizeValue(value));
            if (matches != null) {
                result.addAll(matches);
            }
        }
        return result;
    }
    
    @Override
    public Set<String> findGreaterThan(Object value, boolean inclusive) {
        // Hash index doesn't support range queries efficiently
        log.warn("HashIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findLessThan(Object value, boolean inclusive) {
        log.warn("HashIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findRange(Object from, Object to, boolean fromInclusive, boolean toInclusive) {
        log.warn("HashIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findRegex(java.util.regex.Pattern pattern) {
        // HashIndex doesn't support regex efficiently - return null to indicate full scan needed
        log.debug("HashIndex does not support regex queries efficiently. Use TRIGRAM index for field: {}", 
            definition.getField());
        return null;
    }
    
    @Override
    public Set<String> findPrefix(String prefix) {
        // HashIndex doesn't support prefix queries efficiently - return null
        log.debug("HashIndex does not support prefix queries efficiently. Use PREFIX or TRIGRAM index for field: {}", 
            definition.getField());
        return null;
    }
    
    @Override
    public Set<String> findContains(String substring) {
        // HashIndex doesn't support contains queries - return null
        log.debug("HashIndex does not support contains queries. Use TRIGRAM index for field: {}", 
            definition.getField());
        return null;
    }
    
    @Override
    public void clear() {
        index.clear();
        totalEntries.set(0);
        log.debug("Cleared HashIndex for field: {}", definition.getField());
    }
    
    @Override
    public long size() {
        return totalEntries.get();
    }
    
    @Override
    public int uniqueValues() {
        return index.size();
    }
    
    @Override
    public long memoryUsageBytes() {
        // Rough estimate: each entry ~100 bytes (key reference + set overhead)
        // Plus each document key ~50 bytes average
        long uniqueKeys = index.size();
        long totalKeys = totalEntries.get();
        return (uniqueKeys * 100) + (totalKeys * 50);
    }
    
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("field", definition.getField());
        stats.put("type", "HASH");
        stats.put("entries", totalEntries.get());
        stats.put("uniqueValues", index.size());
        stats.put("memoryBytes", memoryUsageBytes());
        stats.put("supportsRangeQueries", false);
        
        // Value distribution (top 10)
        List<Map<String, Object>> distribution = new ArrayList<>();
        index.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
            .limit(10)
            .forEach(entry -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("value", String.valueOf(entry.getKey()));
                item.put("count", entry.getValue().size());
                item.put("percentage", String.format("%.1f%%", 
                    (entry.getValue().size() * 100.0) / Math.max(1, totalEntries.get())));
                distribution.add(item);
            });
        stats.put("distribution", distribution);
        
        return stats;
    }
    
    @Override
    public boolean supportsRangeQueries() {
        return false;
    }
    
    /**
     * Normalize field value for consistent indexing.
     */
    private Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        // Convert to string for consistent key comparison
        return value.toString();
    }
    
    /**
     * Get all indexed values (for persistence).
     */
    public Map<Object, Set<String>> getAllEntries() {
        return new HashMap<>(index);
    }
    
    /**
     * Load entries from persistence.
     */
    public void loadEntries(Map<Object, Set<String>> entries) {
        clear();
        for (Map.Entry<Object, Set<String>> entry : entries.entrySet()) {
            Set<String> keys = ConcurrentHashMap.newKeySet();
            keys.addAll(entry.getValue());
            index.put(entry.getKey(), keys);
            totalEntries.addAndGet(entry.getValue().size());
        }
        log.debug("Loaded {} entries into HashIndex for field: {}", 
            totalEntries.get(), definition.getField());
    }
}
