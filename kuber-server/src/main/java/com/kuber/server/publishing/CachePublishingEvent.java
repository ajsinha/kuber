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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

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
 * @version 2.5.0
 */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CachePublishingEvent {
    
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    
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
        DELETED("deleted"),
        EXPIRED("expired"),
        QUERY_RESULT("queryresult"),
        AUTOLOAD_START("autoload_start"),
        AUTOLOAD_END("autoload_end");
        
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
     * Create an event for TTL expiration.
     */
    public static CachePublishingEvent expired(String region, String key, String nodeId) {
        return CachePublishingEvent.builder()
                .key(key)
                .action(Action.EXPIRED.getValue())
                .region(region)
                .payload(null)
                .timestamp(Instant.now())
                .nodeId(nodeId)
                .build();
    }
    
    /**
     * Create an event for query result publishing.
     */
    public static CachePublishingEvent queryResult(String region, String key, String value, String nodeId) {
        return CachePublishingEvent.builder()
                .key(key)
                .action(Action.QUERY_RESULT.getValue())
                .region(region)
                .payload(parsePayload(value))
                .timestamp(Instant.now())
                .nodeId(nodeId)
                .build();
    }
    
    /**
     * Create an event for autoload start.
     * 
     * @param region Region being loaded
     * @param fileName Source file name
     * @param nodeId Node ID
     * @return autoload_start event with file metadata in payload
     */
    public static CachePublishingEvent autoloadStart(String region, String fileName, String nodeId) {
        Map<String, Object> meta = new java.util.LinkedHashMap<>();
        meta.put("file", fileName);
        meta.put("region", region);
        return CachePublishingEvent.builder()
                .key(fileName)
                .action(Action.AUTOLOAD_START.getValue())
                .region(region)
                .payload(meta)
                .timestamp(Instant.now())
                .nodeId(nodeId)
                .build();
    }
    
    /**
     * Create an event for autoload end.
     * 
     * @param region Region that was loaded
     * @param fileName Source file name
     * @param recordsLoaded Number of records loaded
     * @param errors Number of errors
     * @param durationMs Processing duration in milliseconds
     * @param status Outcome status (e.g. "SUCCESS", "ERROR")
     * @param nodeId Node ID
     * @return autoload_end event with result metadata in payload
     */
    public static CachePublishingEvent autoloadEnd(String region, String fileName, 
            int recordsLoaded, int errors, long durationMs, String status, String nodeId) {
        Map<String, Object> meta = new java.util.LinkedHashMap<>();
        meta.put("file", fileName);
        meta.put("region", region);
        meta.put("records_loaded", recordsLoaded);
        meta.put("errors", errors);
        meta.put("duration_ms", durationMs);
        meta.put("status", status);
        return CachePublishingEvent.builder()
                .key(fileName)
                .action(Action.AUTOLOAD_END.getValue())
                .region(region)
                .payload(meta)
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
