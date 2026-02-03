/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Patent Pending
 */
package com.kuber.server.replication;

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a single mutation in the Replication OpLog.
 * 
 * <p>Every write operation on the PRIMARY (set, delete, region create/delete/purge)
 * produces one OpLogEntry. The SECONDARY consumes these entries in sequence order
 * to replicate the PRIMARY's state.
 * 
 * <p>For SET operations, the full {@link CacheEntry} is captured at append time
 * so the entry is self-contained and immune to later mutations or evictions.
 * 
 * @since 1.8.3
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ReplicationOpLogEntry {
    
    /**
     * Type of mutation recorded in this entry.
     */
    public enum OpType {
        /** Key was created or updated (entry field populated) */
        SET,
        /** Key was deleted */
        DELETE,
        /** Region was created (regionDef field populated) */
        REGION_CREATE,
        /** Region was deleted (all entries removed) */
        REGION_DELETE,
        /** Region was purged (entries cleared, region kept) */
        REGION_PURGE
    }
    
    /** Monotonically increasing sequence number assigned by the OpLog */
    private long sequenceId;
    
    /** Timestamp when this entry was appended */
    private Instant timestamp;
    
    /** Type of mutation */
    private OpType opType;
    
    /** Region name */
    private String region;
    
    /** Cache key (null for region-level operations) */
    private String key;
    
    /** Full cache entry snapshot (non-null for SET operations) */
    private CacheEntry entry;
    
    /** Region definition snapshot (non-null for REGION_CREATE) */
    private CacheRegion regionDef;
    
    // ==================== Factory Methods ====================
    
    public static ReplicationOpLogEntry set(String region, String key, CacheEntry entry) {
        return ReplicationOpLogEntry.builder()
                .opType(OpType.SET)
                .region(region)
                .key(key)
                .entry(entry)
                .build();
    }
    
    public static ReplicationOpLogEntry delete(String region, String key) {
        return ReplicationOpLogEntry.builder()
                .opType(OpType.DELETE)
                .region(region)
                .key(key)
                .build();
    }
    
    public static ReplicationOpLogEntry regionCreate(CacheRegion regionDef) {
        return ReplicationOpLogEntry.builder()
                .opType(OpType.REGION_CREATE)
                .region(regionDef.getName())
                .regionDef(regionDef)
                .build();
    }
    
    public static ReplicationOpLogEntry regionDelete(String region) {
        return ReplicationOpLogEntry.builder()
                .opType(OpType.REGION_DELETE)
                .region(region)
                .build();
    }
    
    public static ReplicationOpLogEntry regionPurge(String region) {
        return ReplicationOpLogEntry.builder()
                .opType(OpType.REGION_PURGE)
                .region(region)
                .build();
    }
}
