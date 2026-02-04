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
package com.kuber.server.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Represents a cache request received via message broker.
 * 
 * <p>All requests must contain:
 * <ul>
 *   <li>api_key - API key for authentication</li>
 *   <li>message_id - Unique identifier for request/response correlation</li>
 *   <li>operation - Cache operation to perform (GET, SET, DELETE, etc.)</li>
 * </ul>
 * 
 * <p>Supported operations:
 * <ul>
 *   <li>GET - Get value by key</li>
 *   <li>SET - Set key-value pair</li>
 *   <li>DELETE - Delete key(s)</li>
 *   <li>EXISTS - Check if key exists</li>
 *   <li>KEYS - List keys matching pattern</li>
 *   <li>MGET - Get multiple keys</li>
 *   <li>MSET - Set multiple key-value pairs</li>
 *   <li>TTL - Get time-to-live</li>
 *   <li>EXPIRE - Set expiration</li>
 *   <li>HGET - Get hash field</li>
 *   <li>HSET - Set hash field</li>
 *   <li>HGETALL - Get all hash fields</li>
 *   <li>JSET - Set JSON document</li>
 *   <li>JGET - Get JSON document</li>
 *   <li>JUPDATE - Update/merge JSON document (upsert with deep merge)</li>
 *   <li>JREMOVE - Remove attributes from JSON document</li>
 *   <li>JSEARCH - Search JSON documents</li>
 *   <li>PING - Test connection</li>
 *   <li>INFO - Get server info</li>
 *   <li>REGIONS - List regions</li>
 * </ul>
 * 
 * @version 2.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CacheRequest {
    
    /**
     * API key for authentication.
     */
    @JsonProperty("api_key")
    private String apiKey;
    
    /**
     * Unique message ID for request/response correlation.
     */
    @JsonProperty("message_id")
    private String messageId;
    
    /**
     * Cache operation to perform.
     */
    private String operation;
    
    /**
     * Region for the operation (defaults to "default").
     */
    private String region = "default";
    
    /**
     * Key for single-key operations.
     */
    private String key;
    
    /**
     * Value for SET operations.
     */
    private Object value;
    
    /**
     * Keys for multi-key operations (MGET, DELETE multiple).
     */
    private List<String> keys;
    
    /**
     * Key-value pairs for MSET operations.
     */
    private Map<String, Object> entries;
    
    /**
     * Pattern for KEYS operation.
     */
    private String pattern;
    
    /**
     * TTL in seconds for SET/EXPIRE operations.
     */
    private Long ttl;
    
    /**
     * Hash field name for hash operations.
     */
    private String field;
    
    /**
     * Hash field-value pairs for HMSET.
     */
    private Map<String, String> fields;
    
    /**
     * JSON path for JGET operation.
     */
    private String path;
    
    /**
     * Query for JSEARCH operation.
     */
    private String query;
    
    /**
     * Validate the request has required fields.
     */
    public boolean isValid() {
        return apiKey != null && !apiKey.isEmpty() 
            && messageId != null && !messageId.isEmpty()
            && operation != null && !operation.isEmpty();
    }
    
    /**
     * Get validation error message.
     */
    public String getValidationError() {
        if (apiKey == null || apiKey.isEmpty()) {
            return "api_key is required";
        }
        if (messageId == null || messageId.isEmpty()) {
            return "message_id is required";
        }
        if (operation == null || operation.isEmpty()) {
            return "operation is required";
        }
        return null;
    }
    
    /**
     * Check if this is a batch GET operation (MGET with keys array).
     */
    public boolean isBatchGet() {
        return keys != null && !keys.isEmpty();
    }
    
    /**
     * Check if this is a batch SET operation (MSET with entries map).
     */
    public boolean isBatchSet() {
        return entries != null && !entries.isEmpty();
    }
    
    /**
     * Get the batch size for MGET operations.
     */
    public int getBatchGetSize() {
        return keys != null ? keys.size() : 0;
    }
    
    /**
     * Get the batch size for MSET operations.
     */
    public int getBatchSetSize() {
        return entries != null ? entries.size() : 0;
    }
}
