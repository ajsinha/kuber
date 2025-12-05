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
package com.kuber.server.persistence;

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract persistence store interface.
 * Implementations can use MongoDB, SQLite, PostgreSQL, RocksDB, etc.
 */
public interface PersistenceStore {
    
    /**
     * Get the type of this persistence store.
     */
    PersistenceType getType();
    
    /**
     * Initialize the persistence store.
     * Called once at startup.
     */
    void initialize();
    
    /**
     * Shutdown the persistence store.
     * Called once at shutdown.
     */
    void shutdown();
    
    /**
     * Check if the persistence store is available and connected.
     */
    boolean isAvailable();
    
    // ==================== Region Operations ====================
    
    /**
     * Save a region (insert or update).
     */
    void saveRegion(CacheRegion region);
    
    /**
     * Load all regions.
     */
    List<CacheRegion> loadAllRegions();
    
    /**
     * Load a single region by name.
     */
    CacheRegion loadRegion(String name);
    
    /**
     * Delete a region and all its entries.
     */
    void deleteRegion(String name);
    
    /**
     * Purge all entries in a region but keep the region.
     */
    void purgeRegion(String name);
    
    // ==================== Entry Operations ====================
    
    /**
     * Save a cache entry (insert or update).
     */
    void saveEntry(CacheEntry entry);
    
    /**
     * Save a cache entry asynchronously.
     */
    CompletableFuture<Void> saveEntryAsync(CacheEntry entry);
    
    /**
     * Save multiple cache entries in batch.
     */
    void saveEntries(List<CacheEntry> entries);
    
    /**
     * Load a single cache entry.
     */
    CacheEntry loadEntry(String region, String key);
    
    /**
     * Load multiple cache entries by keys (batch operation).
     * This is more efficient than calling loadEntry multiple times.
     * @param region Region name
     * @param keys List of keys to load
     * @return Map of key to CacheEntry (missing keys are not included)
     */
    default java.util.Map<String, CacheEntry> loadEntriesByKeys(String region, java.util.List<String> keys) {
        java.util.Map<String, CacheEntry> result = new java.util.HashMap<>();
        for (String key : keys) {
            CacheEntry entry = loadEntry(region, key);
            if (entry != null) {
                result.put(key, entry);
            }
        }
        return result;
    }
    
    /**
     * Load multiple cache entries from a region.
     * @param region Region name
     * @param limit Maximum number of entries to load
     */
    List<CacheEntry> loadEntries(String region, int limit);
    
    /**
     * Delete a single cache entry.
     */
    void deleteEntry(String region, String key);
    
    /**
     * Delete multiple cache entries.
     */
    void deleteEntries(String region, List<String> keys);
    
    /**
     * Count entries in a region.
     */
    long countEntries(String region);
    
    /**
     * Get all keys in a region.
     */
    List<String> getKeys(String region, String pattern, int limit);
    
    /**
     * Get a single entry by key.
     */
    CacheEntry get(String region, String key);
    
    /**
     * Delete expired entries from a region.
     * @param region Region name
     * @return Number of entries deleted
     */
    long deleteExpiredEntries(String region);
    
    /**
     * Delete all expired entries from all regions.
     * @return Total number of entries deleted
     */
    long deleteAllExpiredEntries();
    
    /**
     * Count non-expired entries in a region.
     * WARNING: This can be slow for large datasets as it iterates all entries.
     * Use estimateEntryCount() for dashboard/UI display.
     */
    long countNonExpiredEntries(String region);
    
    /**
     * Fast estimate of entry count in a region.
     * This is approximate but very fast - suitable for dashboard display.
     * For accurate counts, use countNonExpiredEntries() (slower).
     */
    default long estimateEntryCount(String region) {
        return countEntries(region);
    }
    
    /**
     * Get non-expired keys in a region.
     */
    List<String> getNonExpiredKeys(String region, String pattern, int limit);
    
    /**
     * Persistence store types.
     */
    enum PersistenceType {
        MONGODB("mongodb"),
        SQLITE("sqlite"),
        POSTGRESQL("postgresql"),
        ROCKSDB("rocksdb"),
        MEMORY("memory");
        
        private final String value;
        
        PersistenceType(String value) {
            this.value = value;
        }
        
        public String getValue() {
            return value;
        }
        
        public static PersistenceType fromValue(String value) {
            for (PersistenceType type : values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown persistence type: " + value);
        }
    }
}
