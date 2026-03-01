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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * Represents a cache entry in the Kuber distributed cache.
 * Supports both simple key-value pairs and complex JSON documents.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CacheEntry implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Unique identifier for this cache entry
     */
    private String id;
    
    /**
     * The cache key
     */
    private String key;
    
    /**
     * The region this entry belongs to
     */
    private String region;
    
    /**
     * The value type: STRING, JSON, LIST, SET, HASH, ZSET
     */
    private ValueType valueType;
    
    /**
     * String value (for STRING type)
     */
    private String stringValue;
    
    /**
     * JSON value (for JSON type)
     */
    private JsonNode jsonValue;
    
    /**
     * Raw bytes for binary data
     */
    private byte[] rawValue;
    
    /**
     * Time-to-live in seconds. -1 means no expiration.
     */
    @Builder.Default
    private long ttlSeconds = -1;
    
    /**
     * Timestamp when this entry was created
     */
    private Instant createdAt;
    
    /**
     * Timestamp when this entry was last updated
     */
    private Instant updatedAt;
    
    /**
     * Timestamp when this entry expires (null if no expiration)
     */
    private Instant expiresAt;
    
    /**
     * Version for optimistic locking
     */
    @Builder.Default
    private long version = 1;
    
    /**
     * Number of times this entry has been accessed
     */
    @Builder.Default
    private long accessCount = 0;
    
    /**
     * Last access timestamp
     */
    private Instant lastAccessedAt;
    
    /**
     * Metadata for additional information
     */
    private String metadata;
    
    /**
     * Check if this entry has expired
     */
    public boolean isExpired() {
        if (expiresAt == null) {
            return false;
        }
        return Instant.now().isAfter(expiresAt);
    }
    
    /**
     * Get remaining TTL in seconds
     */
    public long getRemainingTtl() {
        if (expiresAt == null) {
            return -1;
        }
        long remaining = expiresAt.getEpochSecond() - Instant.now().getEpochSecond();
        return Math.max(0, remaining);
    }
    
    /**
     * Increment access count and update last accessed time
     */
    public void recordAccess() {
        this.accessCount++;
        this.lastAccessedAt = Instant.now();
    }
    
    /**
     * Create a copy of this entry for the given region
     */
    public CacheEntry copyToRegion(String newRegion) {
        return CacheEntry.builder()
                .id(this.id)
                .key(this.key)
                .region(newRegion)
                .valueType(this.valueType)
                .stringValue(this.stringValue)
                .jsonValue(this.jsonValue)
                .rawValue(this.rawValue)
                .ttlSeconds(this.ttlSeconds)
                .createdAt(this.createdAt)
                .updatedAt(this.updatedAt)
                .expiresAt(this.expiresAt)
                .version(this.version)
                .accessCount(this.accessCount)
                .lastAccessedAt(this.lastAccessedAt)
                .metadata(this.metadata)
                .build();
    }
    
    /**
     * Value types supported by Kuber cache
     */
    public enum ValueType {
        STRING,
        JSON,
        LIST,
        SET,
        HASH,
        ZSET,
        BINARY
    }
}
