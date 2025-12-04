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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Request DTO for the generic search API endpoint.
 * 
 * Supports multiple search modes:
 * 
 * 1. Simple key lookup:
 *    {"region": "test", "key": "ABC"}
 * 
 * 2. Key pattern search (regex):
 *    {"region": "test", "keypattern": "ABC.*"}
 * 
 * 3. JSON attribute search with equality:
 *    {"region": "test", "type": "json", "values": [{"k1": "abc"}, {"k2": "def"}]}
 * 
 * 4. JSON attribute search with regex:
 *    {"region": "test", "type": "json", "values": [{"k1": "abc", "type": "regex"}, {"k2": "def"}]}
 * 
 * 5. JSON attribute search with IN operator:
 *    {"region": "test", "type": "json", "values": [{"k1": ["abc", "def"]}, {"k2": "ghi"}]}
 * 
 * All search modes support optional field projection:
 *    {"region": "test", "key": "ABC", "fields": ["name", "email", "address.city"]}
 */
@Data
public class GenericSearchRequest {
    
    /**
     * The region to search in. Required.
     */
    private String region;
    
    /**
     * Exact key to lookup. Returns single result if found.
     */
    private String key;
    
    /**
     * Regex pattern to match keys. Returns all matching key-value pairs.
     */
    @JsonProperty("keypattern")
    private String keyPattern;
    
    /**
     * Search type. Set to "json" for JSON attribute search.
     */
    private String type;
    
    /**
     * List of attribute conditions for JSON search.
     * Each element is a map representing a condition:
     * - Simple equality: {"fieldName": "value"}
     * - Regex match: {"fieldName": "pattern", "type": "regex"}
     * - IN operator: {"fieldName": ["value1", "value2", "value3"]}
     */
    private List<Map<String, Object>> values;
    
    /**
     * Optional list of fields to return from JSON/Map objects.
     * Supports nested paths with dot notation (e.g., "address.city").
     * If not specified, all fields are returned.
     */
    private List<String> fields;
    
    /**
     * Maximum number of results to return. Default is 1000.
     */
    private Integer limit;
    
    /**
     * Check if this is a simple key lookup.
     */
    public boolean isKeyLookup() {
        return key != null && !key.isEmpty();
    }
    
    /**
     * Check if this is a key pattern (regex) search.
     */
    public boolean isKeyPatternSearch() {
        return keyPattern != null && !keyPattern.isEmpty();
    }
    
    /**
     * Check if this is a JSON attribute search.
     */
    public boolean isJsonSearch() {
        return "json".equalsIgnoreCase(type) && values != null && !values.isEmpty();
    }
    
    /**
     * Check if field projection is requested.
     */
    public boolean hasFieldProjection() {
        return fields != null && !fields.isEmpty();
    }
    
    /**
     * Get the limit with default value.
     */
    public int getLimitOrDefault() {
        return limit != null && limit > 0 ? limit : 1000;
    }
}
