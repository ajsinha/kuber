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
import com.kuber.core.model.KeyIndexEntry;
import com.kuber.core.model.KeyIndexEntry.ValueLocation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for key index implementations.
 * 
 * Supports both on-heap (KeyIndex) and off-heap (OffHeapKeyIndex) implementations.
 * 
 * @version 2.6.4
 */
public interface KeyIndexInterface {
    
    /**
     * Get the region name.
     */
    String getRegion();
    
    /**
     * Get exact count of keys - O(1).
     */
    int size();
    
    /**
     * Check if index is empty.
     */
    boolean isEmpty();
    
    /**
     * Check if key exists - O(1).
     */
    boolean containsKey(String key);
    
    /**
     * Get key entry with statistics tracking.
     */
    KeyIndexEntry get(String key);
    
    /**
     * Get key entry without updating statistics.
     */
    KeyIndexEntry getWithoutTracking(String key);
    
    /**
     * Put a key entry.
     */
    void put(String key, KeyIndexEntry entry);
    
    /**
     * Put from CacheEntry.
     */
    void putFromCacheEntry(CacheEntry cacheEntry, ValueLocation location);
    
    /**
     * Update value location for a key.
     */
    void updateLocation(String key, ValueLocation newLocation);
    
    /**
     * Remove a key from the index.
     */
    KeyIndexEntry remove(String key);
    
    /**
     * Get all keys.
     */
    Set<String> getAllKeys();
    
    /**
     * Find keys matching a glob pattern.
     */
    List<String> findKeysByPattern(String pattern);
    
    /**
     * Get keys with values in memory.
     */
    List<String> getKeysInMemory();
    
    /**
     * Get keys with values only on disk.
     */
    List<String> getKeysOnDiskOnly();
    
    /**
     * Find expired keys.
     */
    List<String> findExpiredKeys();
    
    /**
     * Clear all entries.
     */
    void clear();
    
    /**
     * Get statistics about this index.
     */
    Map<String, Object> getStatistics();
    
    /**
     * Get hit rate as decimal (0.0 to 1.0).
     */
    double getHitRate();
    
    /**
     * Get memory entry count.
     */
    long getMemoryEntryCount();
    
    /**
     * Get disk-only entry count.
     */
    long getDiskOnlyEntryCount();
    
    /**
     * Check if this is an off-heap implementation.
     */
    default boolean isOffHeap() {
        return false;
    }
    
    /**
     * Get off-heap bytes used (0 for on-heap implementations).
     */
    default long getOffHeapBytesUsed() {
        return 0;
    }
    
    /**
     * Shutdown and release resources.
     */
    default void shutdown() {
        clear();
    }
}
