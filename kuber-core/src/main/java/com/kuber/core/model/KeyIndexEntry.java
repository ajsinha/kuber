/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.core.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Lightweight in-memory index entry for hybrid storage architecture.
 * Contains key metadata while actual values may be stored on disk.
 * 
 * This enables:
 * - O(1) key existence checks (no disk I/O)
 * - Fast KEYS pattern matching (memory-only scan)
 * - Immediate negative lookups (key not in index = doesn't exist)
 * - Exact entry counts (index.size())
 * 
 * Memory footprint per entry (approximate):
 * - key: ~40 bytes average (String object + chars)
 * - valueType: ~8 bytes (enum reference)
 * - valueLocation: ~8 bytes (enum reference)
 * - timestamps: ~24 bytes (3 x long)
 * - ttl/size/version: ~24 bytes (3 x long)
 * - Total: ~104 bytes per key (vs ~500+ bytes for full CacheEntry with value)
 * 
 * @version 1.3.6
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyIndexEntry {
    
    /**
     * Location of the value data.
     */
    public enum ValueLocation {
        /** Value is cached in memory (hot data) */
        MEMORY,
        /** Value is only on disk (cold data) */
        DISK,
        /** Value is in both memory and disk (recently written) */
        BOTH,
        /** Value is only in persistence store, not in memory cache (used during bulk loads) */
        PERSISTENCE_ONLY
    }
    
    /** The cache key */
    private String key;
    
    /** Region this key belongs to */
    private String region;
    
    /** Type of value stored */
    private CacheEntry.ValueType valueType;
    
    /** Where the value is currently stored */
    private ValueLocation valueLocation;
    
    /** Size of the value in bytes (for memory management) */
    private long valueSize;
    
    /** TTL in seconds (-1 for no expiration) */
    private long ttlSeconds;
    
    /** When this entry expires (null for no expiration) */
    private Instant expiresAt;
    
    /** When this entry was created */
    private Instant createdAt;
    
    /** When this entry was last updated */
    private Instant updatedAt;
    
    /** When this entry was last accessed */
    private Instant lastAccessedAt;
    
    /** Number of times this entry has been accessed */
    private long accessCount;
    
    /** Version for optimistic locking */
    private long version;
    
    /**
     * Check if this entry has expired.
     */
    public boolean isExpired() {
        if (expiresAt == null) {
            return false;
        }
        return Instant.now().isAfter(expiresAt);
    }
    
    /**
     * Check if value is available in memory.
     */
    public boolean isValueInMemory() {
        return valueLocation == ValueLocation.MEMORY || valueLocation == ValueLocation.BOTH;
    }
    
    /**
     * Check if value is persisted to disk.
     */
    public boolean isValueOnDisk() {
        return valueLocation == ValueLocation.DISK || valueLocation == ValueLocation.BOTH;
    }
    
    /**
     * Record an access to this entry.
     */
    public void recordAccess() {
        this.lastAccessedAt = Instant.now();
        this.accessCount++;
    }
    
    /**
     * Create index entry from a full CacheEntry.
     */
    public static KeyIndexEntry fromCacheEntry(CacheEntry entry, ValueLocation location) {
        long valueSize = 0;
        if (entry.getStringValue() != null) {
            valueSize = entry.getStringValue().length() * 2L; // chars are 2 bytes
        } else if (entry.getJsonValue() != null) {
            valueSize = entry.getJsonValue().toString().length() * 2L;
        }
        
        return KeyIndexEntry.builder()
                .key(entry.getKey())
                .region(entry.getRegion())
                .valueType(entry.getValueType())
                .valueLocation(location)
                .valueSize(valueSize)
                .ttlSeconds(entry.getTtlSeconds())
                .expiresAt(entry.getExpiresAt())
                .createdAt(entry.getCreatedAt())
                .updatedAt(entry.getUpdatedAt())
                .lastAccessedAt(entry.getLastAccessedAt())
                .accessCount(entry.getAccessCount())
                .version(entry.getVersion())
                .build();
    }
    
    /**
     * Update location when value is evicted from memory to disk.
     */
    public void evictToDisK() {
        this.valueLocation = ValueLocation.DISK;
    }
    
    /**
     * Update location when value is loaded back into memory.
     */
    public void loadToMemory() {
        this.valueLocation = ValueLocation.BOTH;
    }
    
    /**
     * Update location when value is written to both memory and disk.
     */
    public void writeToBoth() {
        this.valueLocation = ValueLocation.BOTH;
    }
}
