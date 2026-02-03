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
package com.kuber.server.dto;

import lombok.Data;

/**
 * Request DTO for the Generic Update API endpoint (v1.7.9).
 * 
 * Provides a unified SET/UPDATE operation via REST API with smart handling
 * of JSON data merging.
 * 
 * Basic Usage:
 * <pre>
 * POST /api/genericupdate
 * {
 *     "apiKey": "kub_xxxxx",
 *     "region": "myregion",
 *     "key": "user:123",
 *     "value": {"name": "John", "age": 30},
 *     "type": "json",
 *     "ttl": 3600
 * }
 * </pre>
 * 
 * Behavior:
 * 1. If key does NOT exist:
 *    - Creates a new entry with the given value
 *    - If type="json", stores as JSON data type
 *    - Applies TTL if provided, otherwise sets TTL to -1 (no expiry)
 * 
 * 2. If key EXISTS:
 *    - If type != "json": Replaces the value entirely
 *    - If type = "json": Performs JUPDATE (merge/update JSON fields)
 *      - Existing fields not in request are preserved
 *      - Fields in request overwrite existing values
 *      - New fields are added
 * 
 * Example - Create new entry:
 * <pre>
 * {"apiKey": "kub_xxx", "region": "users", "key": "user:1", 
 *  "value": {"name": "John", "email": "john@example.com"}, "type": "json"}
 * </pre>
 * 
 * Example - Update existing JSON (merge):
 * <pre>
 * // Existing: {"name": "John", "email": "john@example.com", "age": 25}
 * // Request:
 * {"apiKey": "kub_xxx", "region": "users", "key": "user:1",
 *  "value": {"email": "john.new@example.com", "city": "NYC"}, "type": "json"}
 * // Result: {"name": "John", "email": "john.new@example.com", "age": 25, "city": "NYC"}
 * </pre>
 * 
 * Example - Replace value (non-JSON):
 * <pre>
 * {"apiKey": "kub_xxx", "region": "counters", "key": "hits", "value": 42}
 * </pre>
 * 
 * @version 1.9.0
 */
@Data
public class GenericUpdateRequest {
    
    /**
     * API Key for authentication. Required.
     */
    private String apiKey;
    
    /**
     * The region to update in. Required.
     */
    private String region;
    
    /**
     * The key to set or update. Required.
     */
    private String key;
    
    /**
     * The value to set or merge. Required.
     * Can be:
     * - A scalar value (String, Number, Boolean)
     * - A Map/Object for JSON data
     * - A List/Array
     */
    private Object value;
    
    /**
     * Data type. Optional.
     * Set to "json" for JSON handling with JUPDATE merge semantics.
     * Any other value or null means simple value replacement.
     */
    private String type;
    
    /**
     * Time-to-live in seconds. Optional.
     * If not provided or <= 0, defaults to -1 (no expiry).
     * Only applied when creating new entries.
     */
    private Long ttl;
    
    /**
     * Check if API key is present.
     */
    public boolean hasApiKey() {
        return apiKey != null && !apiKey.isEmpty();
    }
    
    /**
     * Check if this is a JSON type request.
     */
    public boolean isJsonType() {
        return "json".equalsIgnoreCase(type);
    }
    
    /**
     * Check if key is provided.
     */
    public boolean hasKey() {
        return key != null && !key.isEmpty();
    }
    
    /**
     * Check if region is provided.
     */
    public boolean hasRegion() {
        return region != null && !region.isEmpty();
    }
    
    /**
     * Check if value is provided.
     */
    public boolean hasValue() {
        return value != null;
    }
    
    /**
     * Get TTL with default value.
     * Returns -1 if TTL is not set or <= 0.
     */
    public long getTtlOrDefault() {
        return (ttl != null && ttl > 0) ? ttl : -1L;
    }
    
    /**
     * Validate the request has required fields.
     */
    public boolean isValid() {
        return hasApiKey() && hasRegion() && hasKey() && hasValue();
    }
    
    /**
     * Get validation error message if request is invalid.
     */
    public String getValidationError() {
        if (!hasApiKey()) return "apiKey is required";
        if (!hasRegion()) return "region is required";
        if (!hasKey()) return "key is required";
        if (!hasValue()) return "value is required";
        return null;
    }
}
