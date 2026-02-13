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
package com.kuber.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * Represents a cache event for pub/sub functionality.
 * Supports region, entry, and key/value events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CacheEvent implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Unique event ID
     */
    private String eventId;
    
    /**
     * Type of event
     */
    private EventType eventType;
    
    /**
     * Region affected
     */
    private String region;
    
    /**
     * Key affected (for entry events)
     */
    private String key;
    
    /**
     * Old value (for modify events)
     */
    private String oldValue;
    
    /**
     * New value (for set/modify events)
     */
    private String newValue;
    
    /**
     * TTL in seconds (for TTL-related events)
     */
    private Long ttlSeconds;
    
    /**
     * Timestamp when event occurred
     */
    private Instant timestamp;
    
    /**
     * Source node ID
     */
    private String sourceNodeId;
    
    /**
     * Client ID that triggered the event
     */
    private String clientId;
    
    /**
     * Additional metadata
     */
    private String metadata;
    
    /**
     * Event types supported by Kuber
     */
    public enum EventType {
        // Region events
        REGION_CREATED,
        REGION_DELETED,
        REGION_PURGED,
        REGION_UPDATED,
        
        // Entry events
        ENTRY_SET,
        ENTRY_UPDATED,
        ENTRY_DELETED,
        ENTRY_EXPIRED,
        
        // Key/Value events
        KEY_SET,
        KEY_DELETED,
        KEY_EXPIRED,
        KEY_RENAMED,
        
        // TTL events
        TTL_SET,
        TTL_REMOVED,
        
        // Replication events
        SYNC_STARTED,
        SYNC_COMPLETED,
        SYNC_FAILED,
        
        // Node events
        NODE_PRIMARY,
        NODE_SECONDARY,
        NODE_DISCONNECTED
    }
    
    /**
     * Get the event channel name for pub/sub
     */
    public String getChannel() {
        switch (eventType) {
            case REGION_CREATED:
            case REGION_DELETED:
            case REGION_PURGED:
            case REGION_UPDATED:
                return "__kuber__:region:" + region;
                
            case ENTRY_SET:
            case ENTRY_UPDATED:
            case ENTRY_DELETED:
            case ENTRY_EXPIRED:
                return "__kuber__:entry:" + region + ":" + key;
                
            case KEY_SET:
            case KEY_DELETED:
            case KEY_EXPIRED:
            case KEY_RENAMED:
                return "__kuber__:key:" + region + ":" + key;
                
            case TTL_SET:
            case TTL_REMOVED:
                return "__kuber__:ttl:" + region + ":" + key;
                
            case SYNC_STARTED:
            case SYNC_COMPLETED:
            case SYNC_FAILED:
                return "__kuber__:sync";
                
            case NODE_PRIMARY:
            case NODE_SECONDARY:
            case NODE_DISCONNECTED:
                return "__kuber__:node";
                
            default:
                return "__kuber__:event";
        }
    }
    
    /**
     * Check if this is a region-level event
     */
    public boolean isRegionEvent() {
        return eventType == EventType.REGION_CREATED ||
               eventType == EventType.REGION_DELETED ||
               eventType == EventType.REGION_PURGED ||
               eventType == EventType.REGION_UPDATED;
    }
    
    /**
     * Check if this is an entry-level event
     */
    public boolean isEntryEvent() {
        return eventType == EventType.ENTRY_SET ||
               eventType == EventType.ENTRY_UPDATED ||
               eventType == EventType.ENTRY_DELETED ||
               eventType == EventType.ENTRY_EXPIRED;
    }
    
    /**
     * Check if this is a key-level event
     */
    public boolean isKeyEvent() {
        return eventType == EventType.KEY_SET ||
               eventType == EventType.KEY_DELETED ||
               eventType == EventType.KEY_EXPIRED ||
               eventType == EventType.KEY_RENAMED;
    }
    
    /**
     * Factory method for region creation event
     */
    public static CacheEvent regionCreated(String region, String sourceNodeId) {
        return CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(EventType.REGION_CREATED)
                .region(region)
                .timestamp(Instant.now())
                .sourceNodeId(sourceNodeId)
                .build();
    }
    
    /**
     * Factory method for region deletion event
     */
    public static CacheEvent regionDeleted(String region, String sourceNodeId) {
        return CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(EventType.REGION_DELETED)
                .region(region)
                .timestamp(Instant.now())
                .sourceNodeId(sourceNodeId)
                .build();
    }
    
    /**
     * Factory method for entry set event
     */
    public static CacheEvent entrySet(String region, String key, String value, String sourceNodeId) {
        return CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(EventType.ENTRY_SET)
                .region(region)
                .key(key)
                .newValue(value)
                .timestamp(Instant.now())
                .sourceNodeId(sourceNodeId)
                .build();
    }
    
    /**
     * Factory method for entry deleted event
     */
    public static CacheEvent entryDeleted(String region, String key, String sourceNodeId) {
        return CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(EventType.ENTRY_DELETED)
                .region(region)
                .key(key)
                .timestamp(Instant.now())
                .sourceNodeId(sourceNodeId)
                .build();
    }
    
    /**
     * Factory method for entry expired event
     */
    public static CacheEvent entryExpired(String region, String key, String sourceNodeId) {
        return CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(EventType.ENTRY_EXPIRED)
                .region(region)
                .key(key)
                .timestamp(Instant.now())
                .sourceNodeId(sourceNodeId)
                .build();
    }
}
