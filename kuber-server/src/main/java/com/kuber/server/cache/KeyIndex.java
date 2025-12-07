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

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.KeyIndexEntry;
import com.kuber.core.model.KeyIndexEntry.ValueLocation;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * In-memory key index for a single region (ON-HEAP).
 * 
 * This class maintains an index of ALL keys in a region, enabling:
 * - O(1) key existence checks
 * - O(n) key pattern matching (n = number of keys, not disk entries)
 * - Immediate negative lookups
 * - Exact entry counts
 * 
 * Thread-safe using ConcurrentHashMap.
 * 
 * For off-heap storage, use OffHeapKeyIndex instead.
 * 
 * @version 1.3.9
 */
@Slf4j
public class KeyIndex implements KeyIndexInterface {
    
    private final String region;
    private final ConcurrentHashMap<String, KeyIndexEntry> index;
    private final AtomicLong totalValueSize;
    private final AtomicLong memoryValueSize;
    
    // Statistics
    private final AtomicLong indexHits;
    private final AtomicLong indexMisses;
    
    public KeyIndex(String region) {
        this.region = region;
        this.index = new ConcurrentHashMap<>();
        this.totalValueSize = new AtomicLong(0);
        this.memoryValueSize = new AtomicLong(0);
        this.indexHits = new AtomicLong(0);
        this.indexMisses = new AtomicLong(0);
    }
    
    /**
     * Get the region name.
     */
    public String getRegion() {
        return region;
    }
    
    /**
     * Get exact count of keys in this region - O(1).
     */
    public int size() {
        return index.size();
    }
    
    /**
     * Check if index is empty.
     */
    public boolean isEmpty() {
        return index.isEmpty();
    }
    
    /**
     * Check if a key exists - O(1), no disk I/O.
     */
    public boolean containsKey(String key) {
        KeyIndexEntry entry = index.get(key);
        if (entry == null) {
            indexMisses.incrementAndGet();
            return false;
        }
        if (entry.isExpired()) {
            // Don't remove here, let expiration service handle it
            indexMisses.incrementAndGet();
            return false;
        }
        indexHits.incrementAndGet();
        return true;
    }
    
    /**
     * Get index entry for a key - O(1).
     * Returns null if key doesn't exist or is expired.
     */
    public KeyIndexEntry get(String key) {
        KeyIndexEntry entry = index.get(key);
        if (entry == null) {
            indexMisses.incrementAndGet();
            return null;
        }
        if (entry.isExpired()) {
            indexMisses.incrementAndGet();
            return null;
        }
        indexHits.incrementAndGet();
        entry.recordAccess();
        return entry;
    }
    
    /**
     * Get index entry without access tracking (for internal use).
     */
    public KeyIndexEntry getWithoutTracking(String key) {
        return index.get(key);
    }
    
    /**
     * Add or update a key in the index.
     */
    public void put(String key, KeyIndexEntry entry) {
        KeyIndexEntry old = index.put(key, entry);
        
        // Update size tracking
        if (old != null) {
            totalValueSize.addAndGet(-old.getValueSize());
            if (old.isValueInMemory()) {
                memoryValueSize.addAndGet(-old.getValueSize());
            }
        }
        totalValueSize.addAndGet(entry.getValueSize());
        if (entry.isValueInMemory()) {
            memoryValueSize.addAndGet(entry.getValueSize());
        }
    }
    
    /**
     * Add or update from a CacheEntry.
     */
    public void putFromCacheEntry(CacheEntry cacheEntry, ValueLocation location) {
        KeyIndexEntry indexEntry = KeyIndexEntry.fromCacheEntry(cacheEntry, location);
        put(cacheEntry.getKey(), indexEntry);
    }
    
    /**
     * Remove a key from the index.
     */
    public KeyIndexEntry remove(String key) {
        KeyIndexEntry removed = index.remove(key);
        if (removed != null) {
            totalValueSize.addAndGet(-removed.getValueSize());
            if (removed.isValueInMemory()) {
                memoryValueSize.addAndGet(-removed.getValueSize());
            }
        }
        return removed;
    }
    
    /**
     * Update value location for a key (e.g., when evicting to disk).
     */
    public void updateLocation(String key, ValueLocation newLocation) {
        KeyIndexEntry entry = index.get(key);
        if (entry != null) {
            ValueLocation oldLocation = entry.getValueLocation();
            entry.setValueLocation(newLocation);
            
            // Update memory size tracking
            if (oldLocation == ValueLocation.MEMORY || oldLocation == ValueLocation.BOTH) {
                if (newLocation == ValueLocation.DISK) {
                    memoryValueSize.addAndGet(-entry.getValueSize());
                }
            } else if (oldLocation == ValueLocation.DISK) {
                if (newLocation == ValueLocation.MEMORY || newLocation == ValueLocation.BOTH) {
                    memoryValueSize.addAndGet(entry.getValueSize());
                }
            }
        }
    }
    
    /**
     * Find all keys matching a glob pattern - O(n) memory scan, no disk I/O.
     */
    public List<String> findKeysByPattern(String pattern) {
        if (pattern == null || pattern.equals("*")) {
            // Return all non-expired keys
            return index.entrySet().stream()
                    .filter(e -> !e.getValue().isExpired())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        }
        
        // Convert glob pattern to regex
        String regex = globToRegex(pattern);
        Pattern compiledPattern = Pattern.compile(regex);
        
        return index.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .map(Map.Entry::getKey)
                .filter(key -> compiledPattern.matcher(key).matches())
                .collect(Collectors.toList());
    }
    
    /**
     * Find keys matching a predicate.
     */
    public List<String> findKeys(Predicate<KeyIndexEntry> predicate) {
        return index.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .filter(e -> predicate.test(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Get all keys (non-expired).
     */
    public Set<String> getAllKeys() {
        return index.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
    
    /**
     * Get all index entries (non-expired).
     */
    public Collection<KeyIndexEntry> getAllEntries() {
        return index.values().stream()
                .filter(e -> !e.isExpired())
                .collect(Collectors.toList());
    }
    
    /**
     * Get keys with values in memory.
     */
    public List<String> getKeysInMemory() {
        return index.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .filter(e -> e.getValue().isValueInMemory())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Get keys with values only on disk.
     */
    public List<String> getKeysOnDiskOnly() {
        return index.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .filter(e -> e.getValue().getValueLocation() == ValueLocation.DISK)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Find expired keys for cleanup.
     */
    public List<String> findExpiredKeys() {
        return index.entrySet().stream()
                .filter(e -> e.getValue().isExpired())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Remove all expired entries from index.
     * Returns list of removed keys.
     */
    public List<String> removeExpiredEntries() {
        List<String> expired = findExpiredKeys();
        for (String key : expired) {
            remove(key);
        }
        return expired;
    }
    
    /**
     * Get keys sorted by last access time (least recently accessed first).
     * Used for LRU eviction.
     */
    public List<String> getKeysByLRU(int limit) {
        return index.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .filter(e -> e.getValue().isValueInMemory())
                .sorted(Comparator.comparing(e -> 
                    e.getValue().getLastAccessedAt() != null ? 
                    e.getValue().getLastAccessedAt() : Instant.EPOCH))
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Get keys sorted by access count (least frequently accessed first).
     * Used for LFU eviction.
     */
    public List<String> getKeysByLFU(int limit) {
        return index.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .filter(e -> e.getValue().isValueInMemory())
                .sorted(Comparator.comparingLong(e -> e.getValue().getAccessCount()))
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Clear all entries from the index.
     */
    public void clear() {
        index.clear();
        totalValueSize.set(0);
        memoryValueSize.set(0);
    }
    
    /**
     * Get total value size across all entries.
     */
    public long getTotalValueSize() {
        return totalValueSize.get();
    }
    
    /**
     * Get total size of values currently in memory.
     */
    public long getMemoryValueSize() {
        return memoryValueSize.get();
    }
    
    /**
     * Get count of entries with values in memory.
     */
    public long getMemoryEntryCount() {
        return index.values().stream()
                .filter(e -> !e.isExpired())
                .filter(KeyIndexEntry::isValueInMemory)
                .count();
    }
    
    /**
     * Get count of entries with values only on disk.
     */
    public long getDiskOnlyEntryCount() {
        return index.values().stream()
                .filter(e -> !e.isExpired())
                .filter(e -> e.getValueLocation() == ValueLocation.DISK)
                .count();
    }
    
    /**
     * Get index statistics.
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("region", region);
        stats.put("totalKeys", size());
        stats.put("keysInMemory", getMemoryEntryCount());
        stats.put("keysOnDiskOnly", getDiskOnlyEntryCount());
        stats.put("totalValueSize", totalValueSize.get());
        stats.put("memoryValueSize", memoryValueSize.get());
        stats.put("indexHits", indexHits.get());
        stats.put("indexMisses", indexMisses.get());
        
        long total = indexHits.get() + indexMisses.get();
        if (total > 0) {
            stats.put("indexHitRate", String.format("%.2f%%", (indexHits.get() * 100.0) / total));
        } else {
            stats.put("indexHitRate", "N/A");
        }
        
        return stats;
    }
    
    /**
     * Get hit rate as a decimal (0.0 to 1.0).
     * Returns 0.0 if no lookups have been performed.
     */
    public double getHitRate() {
        long total = indexHits.get() + indexMisses.get();
        if (total > 0) {
            return (double) indexHits.get() / total;
        }
        return 0.0;
    }
    
    /**
     * Convert glob pattern to regex.
     */
    private String globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
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
                case '[':
                case ']':
                case '{':
                case '}':
                case '\\':
                    regex.append("\\").append(c);
                    break;
                default:
                    regex.append(c);
            }
        }
        regex.append("$");
        return regex.toString();
    }
}
