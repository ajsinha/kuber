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
package com.kuber.server.publishing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Event model for cache operations that are published to Kafka/ActiveMQ.
 * 
 * When a key is inserted, updated, or deleted in a region configured for
 * event publishing, an instance of this class is created and published
 * asynchronously to the configured messaging systems.
 * 
 * JSON Structure for insert/update:
 * {
 *   "key": "user:1001",
 *   "action": "inserted" | "updated",
 *   "region": "customers",
 *   "payload": { ... full value ... },
 *   "timestamp": "2025-12-06T12:00:00Z",
 *   "nodeId": "kuber-01"
 * }
 * 
 * JSON Structure for delete:
 * {
 *   "key": "user:1001",
 *   "action": "deleted",
 *   "region": "customers",
 *   "timestamp": "2025-12-06T12:00:00Z",
 *   "nodeId": "kuber-01"
 * }
 * 
 * @version 1.4.1
 */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CachePublishingEvent {
    
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    /**
     * The cache key that was modified
     */
    private String key;
    
    /**
     * The action performed: "inserted", "updated", or "deleted"
     */
    private String action;
    
    /**
     * The region where the operation occurred
     */
    private String region;
    
    /**
     * The full value/payload (null for delete operations)
     */
    private Object payload;
    
    /**
     * Timestamp when the event occurred
     */
    private Instant timestamp;
    
    /**
     * Node ID where the operation originated
     */
    private String nodeId;
    
    /**
     * Action type enumeration
     */
    public enum Action {
        INSERTED("inserted"),
        UPDATED("updated"),
        DELETED("deleted");
        
        private final String value;
        
        Action(String value) {
            this.value = value;
        }
        
        public String getValue() {
            return value;
        }
    }
    
    /**
     * Create an event for insert operation.
     */
    public static CachePublishingEvent inserted(String region, String key, String value, String nodeId) {
        return CachePublishingEvent.builder()
                .key(key)
                .action(Action.INSERTED.getValue())
                .region(region)
                .payload(parsePayload(value))
                .timestamp(Instant.now())
                .nodeId(nodeId)
                .build();
    }
    
    /**
     * Create an event for update operation.
     */
    public static CachePublishingEvent updated(String region, String key, String value, String nodeId) {
        return CachePublishingEvent.builder()
                .key(key)
                .action(Action.UPDATED.getValue())
                .region(region)
                .payload(parsePayload(value))
                .timestamp(Instant.now())
                .nodeId(nodeId)
                .build();
    }
    
    /**
     * Create an event for delete operation.
     */
    public static CachePublishingEvent deleted(String region, String key, String nodeId) {
        return CachePublishingEvent.builder()
                .key(key)
                .action(Action.DELETED.getValue())
                .region(region)
                .payload(null)
                .timestamp(Instant.now())
                .nodeId(nodeId)
                .build();
    }
    
    /**
     * Parse the value as JSON if possible, otherwise return as string.
     */
    private static Object parsePayload(String value) {
        if (value == null) {
            return null;
        }
        try {
            // Try to parse as JSON
            return OBJECT_MAPPER.readTree(value);
        } catch (Exception e) {
            // Return as plain string
            return value;
        }
    }
    
    /**
     * Serialize this event to JSON string.
     */
    public String toJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize CachePublishingEvent to JSON", e);
        }
    }
    
    /**
     * Deserialize from JSON string.
     */
    public static CachePublishingEvent fromJson(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, CachePublishingEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize CachePublishingEvent from JSON", e);
        }
    }
}
