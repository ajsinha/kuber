/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index.storage;

import java.util.Set;

/**
 * Abstraction for secondary index storage backends.
 * 
 * <p>This interface allows Kuber secondary indexes to be stored in different backends:
 * <ul>
 *   <li><b>Memory</b> - ConcurrentHashMap (fastest, uses heap)</li>
 *   <li><b>OffHeap</b> - Chronicle Map (fast, uses direct memory)</li>
 *   <li><b>RocksDB</b> - Disk-based (slower, minimal RAM, persistent)</li>
 *   <li><b>LMDB</b> - Disk-based (good read performance, memory-mapped)</li>
 *   <li><b>SQLite</b> - Disk-based (portable, SQL-queryable)</li>
 * </ul>
 * 
 * <p>The storage backend is independent of the main data persistence backend.
 * For example, you can use PostgreSQL for data storage and RocksDB for indexes.
 * 
 * <p>Index data structure: {@code indexName:fieldValue → Set<cacheKeys>}
 * 
 * @version 2.3.0
 * @since 1.9.0
 */
public interface IndexStorageProvider {
    
    /**
     * Storage backend types.
     */
    enum StorageType {
        /** In-memory using ConcurrentHashMap */
        MEMORY,
        /** Off-heap using Chronicle Map */
        OFFHEAP,
        /** Disk-based using RocksDB */
        ROCKSDB,
        /** Disk-based using LMDB */
        LMDB,
        /** Disk-based using SQLite */
        SQLITE
    }
    
    /**
     * Add a key to an index entry.
     * 
     * @param indexName  Unique index identifier (e.g., "trades:status:HASH")
     * @param fieldValue The indexed field value
     * @param cacheKey   The cache key to associate with this value
     */
    void addToIndex(String indexName, Object fieldValue, String cacheKey);
    
    /**
     * Remove a key from an index entry.
     * 
     * @param indexName  Unique index identifier
     * @param fieldValue The indexed field value
     * @param cacheKey   The cache key to remove
     */
    void removeFromIndex(String indexName, Object fieldValue, String cacheKey);
    
    /**
     * Get all cache keys for an exact field value.
     * 
     * @param indexName  Unique index identifier
     * @param fieldValue The indexed field value to look up
     * @return Set of cache keys, or empty set if not found
     */
    Set<String> getExact(String indexName, Object fieldValue);
    
    /**
     * Get all cache keys for a range of field values (for B-tree indexes).
     * 
     * @param indexName Unique index identifier
     * @param fromValue Start of range (inclusive), null for unbounded
     * @param toValue   End of range (inclusive), null for unbounded
     * @return Set of cache keys matching the range
     */
    Set<String> getRange(String indexName, Object fromValue, Object toValue);
    
    /**
     * Get all cache keys where field value starts with prefix (for prefix indexes).
     * 
     * @param indexName Unique index identifier
     * @param prefix    The prefix to match
     * @return Set of cache keys matching the prefix
     */
    Set<String> getByPrefix(String indexName, String prefix);
    
    /**
     * Clear all entries for a specific index.
     * 
     * @param indexName Unique index identifier
     */
    void clearIndex(String indexName);
    
    /**
     * Clear all indexes for a region.
     * 
     * @param region Region name
     */
    void clearRegion(String region);
    
    /**
     * Get the count of unique values in an index.
     * 
     * @param indexName Unique index identifier
     * @return Number of unique indexed values
     */
    long getIndexSize(String indexName);
    
    /**
     * Get total number of key mappings in an index.
     * 
     * @param indexName Unique index identifier
     * @return Total number of (value → key) mappings
     */
    long getTotalMappings(String indexName);
    
    /**
     * Check if the storage provider supports range queries efficiently.
     * 
     * @return true if range queries are O(log n), false if O(n)
     */
    boolean supportsRangeQueries();
    
    /**
     * Check if the storage provider supports prefix queries efficiently.
     * 
     * @return true if prefix queries are efficient
     */
    boolean supportsPrefixQueries();
    
    /**
     * Check if index data persists across restarts.
     * 
     * @return true if data survives JVM restart
     */
    boolean isPersistent();
    
    /**
     * Get estimated memory usage in bytes.
     * 
     * @return Estimated memory usage, or -1 if unknown
     */
    long getMemoryUsage();
    
    /**
     * Get the storage type.
     * 
     * @return Storage backend type
     */
    StorageType getStorageType();
    
    /**
     * Initialize the storage provider.
     * Called once during startup.
     */
    void initialize();
    
    /**
     * Shutdown the storage provider.
     * Called during graceful shutdown.
     */
    void shutdown();
    
    /**
     * Flush any buffered writes to disk (for persistent backends).
     */
    default void flush() {
        // No-op for in-memory backends
    }
    
    /**
     * Compact the storage (for disk-based backends).
     */
    default void compact() {
        // No-op for in-memory backends
    }
}
