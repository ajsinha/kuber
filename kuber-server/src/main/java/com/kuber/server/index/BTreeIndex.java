/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 */
package com.kuber.server.index;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * B-Tree based secondary index for range queries.
 * 
 * <p>Provides O(log n) lookup for range queries like:
 * <ul>
 *   <li>age > 30</li>
 *   <li>price BETWEEN 100 AND 500</li>
 *   <li>created_at >= "2025-01-01"</li>
 * </ul>
 * 
 * <p>Also supports equality queries with O(log n) lookup.
 * 
 * <p>Thread-safe implementation using ConcurrentSkipListMap.
 * 
 * @version 2.6.4
 * @since 1.9.0
 */
@Slf4j
public class BTreeIndex implements SecondaryIndex {
    
    private final IndexDefinition definition;
    
    // Sorted map: Comparable fieldValue -> Set of document keys
    // ConcurrentSkipListMap provides thread-safe sorted access
    private final ConcurrentSkipListMap<Comparable<?>, Set<String>> index;
    
    // Statistics
    private final AtomicLong totalEntries = new AtomicLong(0);
    private Comparable<?> minValue = null;
    private Comparable<?> maxValue = null;
    
    public BTreeIndex(IndexDefinition definition) {
        this.definition = definition;
        this.index = new ConcurrentSkipListMap<>(this::compareValues);
        log.debug("Created BTreeIndex for field: {}", definition.getField());
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
        
        Comparable<?> comparableValue = toComparable(fieldValue);
        if (comparableValue == null) {
            log.trace("Cannot convert value to Comparable: {}", fieldValue);
            return;
        }
        
        index.computeIfAbsent(comparableValue, k -> ConcurrentHashMap.newKeySet())
             .add(documentKey);
        totalEntries.incrementAndGet();
        
        // Update min/max
        updateMinMax(comparableValue);
    }
    
    @Override
    public void remove(Object fieldValue, String documentKey) {
        if (fieldValue == null || documentKey == null) {
            return;
        }
        
        Comparable<?> comparableValue = toComparable(fieldValue);
        if (comparableValue == null) {
            return;
        }
        
        Set<String> keys = index.get(comparableValue);
        if (keys != null) {
            if (keys.remove(documentKey)) {
                totalEntries.decrementAndGet();
            }
            if (keys.isEmpty()) {
                index.remove(comparableValue, keys);
            }
        }
    }
    
    @Override
    public void update(Object oldValue, Object newValue, String documentKey) {
        if (documentKey == null) {
            return;
        }
        
        Comparable<?> oldComparable = toComparable(oldValue);
        Comparable<?> newComparable = toComparable(newValue);
        
        if (Objects.equals(oldComparable, newComparable)) {
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
        
        Comparable<?> comparableValue = toComparable(fieldValue);
        if (comparableValue == null) {
            return Collections.emptySet();
        }
        
        Set<String> result = index.get(comparableValue);
        // v2.2.0: Return unmodifiable view instead of defensive copy
        return result != null ? Collections.unmodifiableSet(result) : Collections.emptySet();
    }
    
    @Override
    public Set<String> findIn(Set<Object> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        
        // For IN queries we need to create a new set for union
        Set<String> result = new HashSet<>();
        for (Object value : values) {
            Comparable<?> comparableValue = toComparable(value);
            if (comparableValue != null) {
                Set<String> matches = index.get(comparableValue);
                if (matches != null) {
                    result.addAll(matches);
                }
            }
        }
        return result;
    }
    
    @Override
    public Set<String> findGreaterThan(Object value, boolean inclusive) {
        if (value == null) {
            return Collections.emptySet();
        }
        
        Comparable<?> comparableValue = toComparable(value);
        if (comparableValue == null) {
            return Collections.emptySet();
        }
        
        Set<String> result = new HashSet<>();
        
        // Get all entries greater than (or equal to if inclusive) the value
        NavigableMap<Comparable<?>, Set<String>> tailMap = 
            inclusive ? index.tailMap(comparableValue, true) 
                      : index.tailMap(comparableValue, false);
        
        for (Set<String> keys : tailMap.values()) {
            result.addAll(keys);
        }
        
        return result;
    }
    
    @Override
    public Set<String> findLessThan(Object value, boolean inclusive) {
        if (value == null) {
            return Collections.emptySet();
        }
        
        Comparable<?> comparableValue = toComparable(value);
        if (comparableValue == null) {
            return Collections.emptySet();
        }
        
        Set<String> result = new HashSet<>();
        
        // Get all entries less than (or equal to if inclusive) the value
        NavigableMap<Comparable<?>, Set<String>> headMap = 
            inclusive ? index.headMap(comparableValue, true) 
                      : index.headMap(comparableValue, false);
        
        for (Set<String> keys : headMap.values()) {
            result.addAll(keys);
        }
        
        return result;
    }
    
    @Override
    public Set<String> findRange(Object from, Object to, boolean fromInclusive, boolean toInclusive) {
        if (from == null && to == null) {
            // Return all
            Set<String> result = new HashSet<>();
            for (Set<String> keys : index.values()) {
                result.addAll(keys);
            }
            return result;
        }
        
        if (from == null) {
            return findLessThan(to, toInclusive);
        }
        
        if (to == null) {
            return findGreaterThan(from, fromInclusive);
        }
        
        Comparable<?> fromComparable = toComparable(from);
        Comparable<?> toComparable = toComparable(to);
        
        if (fromComparable == null || toComparable == null) {
            return Collections.emptySet();
        }
        
        Set<String> result = new HashSet<>();
        
        // Get subMap between from and to
        NavigableMap<Comparable<?>, Set<String>> subMap = 
            index.subMap(fromComparable, fromInclusive, toComparable, toInclusive);
        
        for (Set<String> keys : subMap.values()) {
            result.addAll(keys);
        }
        
        return result;
    }
    
    @Override
    public Set<String> findRegex(java.util.regex.Pattern pattern) {
        // BTreeIndex doesn't support regex efficiently - return null to indicate full scan needed
        log.debug("BTreeIndex does not support regex queries efficiently. Use TRIGRAM index for field: {}", 
            definition.getField());
        return null;
    }
    
    @Override
    public Set<String> findPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<String> result = new HashSet<>();
        
        // BTree can do prefix search on string values by using subMap
        // Find first key >= prefix and iterate until key doesn't start with prefix
        String prefixEnd = prefix + Character.MAX_VALUE;
        
        NavigableMap<Comparable<?>, Set<String>> subMap = 
            index.subMap(prefix, true, prefixEnd, false);
        
        for (Map.Entry<Comparable<?>, Set<String>> entry : subMap.entrySet()) {
            if (entry.getKey().toString().startsWith(prefix)) {
                result.addAll(entry.getValue());
            }
        }
        
        return result;
    }
    
    @Override
    public Set<String> findContains(String substring) {
        // BTreeIndex doesn't support contains queries efficiently - return null
        log.debug("BTreeIndex does not support contains queries. Use TRIGRAM index for field: {}", 
            definition.getField());
        return null;
    }
    
    @Override
    public void clear() {
        index.clear();
        totalEntries.set(0);
        minValue = null;
        maxValue = null;
        log.debug("Cleared BTreeIndex for field: {}", definition.getField());
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
        // Rough estimate: Skip list node ~120 bytes + set overhead ~100 bytes per unique value
        // Plus each document key ~50 bytes average
        long uniqueKeys = index.size();
        long totalKeys = totalEntries.get();
        return (uniqueKeys * 220) + (totalKeys * 50);
    }
    
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("field", definition.getField());
        stats.put("type", "BTREE");
        stats.put("entries", totalEntries.get());
        stats.put("uniqueValues", index.size());
        stats.put("memoryBytes", memoryUsageBytes());
        stats.put("supportsRangeQueries", true);
        
        if (minValue != null) {
            stats.put("minValue", minValue.toString());
        }
        if (maxValue != null) {
            stats.put("maxValue", maxValue.toString());
        }
        
        // Value distribution (top 10 by count)
        List<Map<String, Object>> distribution = new ArrayList<>();
        index.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
            .limit(10)
            .forEach(entry -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("value", entry.getKey().toString());
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
        return true;
    }
    
    /**
     * Convert value to a Comparable type for indexing.
     */
    @SuppressWarnings("unchecked")
    private Comparable<?> toComparable(Object value) {
        if (value == null) {
            return null;
        }
        
        // Already Comparable
        if (value instanceof Comparable) {
            // Normalize numbers to BigDecimal for consistent comparison
            if (value instanceof Number) {
                return new BigDecimal(value.toString());
            }
            return (Comparable<?>) value;
        }
        
        // Try to parse as number
        String strValue = value.toString();
        try {
            return new BigDecimal(strValue);
        } catch (NumberFormatException ignored) {
        }
        
        // Try to parse as date/time
        try {
            return LocalDateTime.parse(strValue);
        } catch (DateTimeParseException ignored) {
        }
        
        try {
            return LocalDate.parse(strValue);
        } catch (DateTimeParseException ignored) {
        }
        
        // Fall back to string comparison
        return strValue;
    }
    
    /**
     * Compare two Comparable values safely.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareValues(Comparable a, Comparable b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        
        // Same type - direct comparison
        if (a.getClass().equals(b.getClass())) {
            return a.compareTo(b);
        }
        
        // Different types - compare by string representation
        return a.toString().compareTo(b.toString());
    }
    
    /**
     * Update min/max values.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void updateMinMax(Comparable<?> value) {
        if (minValue == null || compareValues((Comparable) value, (Comparable) minValue) < 0) {
            minValue = value;
        }
        if (maxValue == null || compareValues((Comparable) value, (Comparable) maxValue) > 0) {
            maxValue = value;
        }
    }
    
    /**
     * Get all indexed values (for persistence).
     */
    public Map<Comparable<?>, Set<String>> getAllEntries() {
        return new TreeMap<>(index);
    }
    
    /**
     * Load entries from persistence.
     */
    public void loadEntries(Map<Comparable<?>, Set<String>> entries) {
        clear();
        for (Map.Entry<Comparable<?>, Set<String>> entry : entries.entrySet()) {
            Set<String> keys = ConcurrentHashMap.newKeySet();
            keys.addAll(entry.getValue());
            index.put(entry.getKey(), keys);
            totalEntries.addAndGet(entry.getValue().size());
            updateMinMax(entry.getKey());
        }
        log.debug("Loaded {} entries into BTreeIndex for field: {}", 
            totalEntries.get(), definition.getField());
    }
}
