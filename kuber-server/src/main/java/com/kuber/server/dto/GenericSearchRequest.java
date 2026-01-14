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
 * Request DTO for the enhanced generic search API endpoint (v1.7.9).
 * 
 * Supports multiple search modes with AND logic across all criteria:
 * 
 * 1. Single key lookup:
 *    {"region": "test", "key": "ABC", "apiKey": "kub_xxx"}
 * 
 * 2. Multiple keys lookup:
 *    {"region": "test", "keys": ["ABC", "DEF", "GHI"], "apiKey": "kub_xxx"}
 * 
 * 3. Single key pattern search (regex):
 *    {"region": "test", "keypattern": "ABC.*", "apiKey": "kub_xxx"}
 * 
 * 4. Multiple key patterns search (regex):
 *    {"region": "test", "keypatterns": ["ABC.*", "DEF.*", ".*XYZ"], "apiKey": "kub_xxx"}
 * 
 * 5. JSON attribute search with equality:
 *    {"region": "test", "type": "json", "attributes": {"status": "active"}, "apiKey": "kub_xxx"}
 * 
 * 6. JSON attribute search with value lists (IN operator):
 *    {"region": "test", "type": "json", "attributes": {"status": ["active", "pending"]}, "apiKey": "kub_xxx"}
 * 
 * 7. JSON attribute search with regex:
 *    {"region": "test", "type": "json", "attributes": {"name": {"$regex": "John.*"}}, "apiKey": "kub_xxx"}
 * 
 * 8. Combined JSON search (AND logic):
 *    {"region": "test", "type": "json", "attributes": {
 *        "status": ["active", "pending"],
 *        "city": "NYC",
 *        "email": {"$regex": ".*@gmail.com"}
 *    }, "apiKey": "kub_xxx"}
 * 
 * 9. Combined key patterns with JSON attributes:
 *    {"region": "test", "keypatterns": ["user_.*"], "type": "json", "attributes": {"status": "active"}, "apiKey": "kub_xxx"}
 * 
 * All search modes support optional field projection:
 *    {"region": "test", "key": "ABC", "fields": ["name", "email", "address.city"]}
 */
@Data
public class GenericSearchRequest {
    
    /**
     * API Key for authentication. Can be passed in request body instead of header.
     */
    private String apiKey;
    
    /**
     * The region to search in. Required.
     */
    private String region;
    
    /**
     * Exact key to lookup. Returns single result if found.
     */
    private String key;
    
    /**
     * List of exact keys to lookup. Returns all matching key-value pairs.
     */
    private List<String> keys;
    
    /**
     * Single regex pattern to match keys. Returns all matching key-value pairs.
     */
    @JsonProperty("keypattern")
    private String keyPattern;
    
    /**
     * List of regex patterns to match keys. Returns keys matching ANY pattern.
     */
    @JsonProperty("keypatterns")
    private List<String> keyPatterns;
    
    /**
     * Search type. Set to "json" for JSON attribute search.
     */
    private String type;
    
    /**
     * JSON search criteria map (v1.7.9 enhanced format).
     * Supports:
     * - Simple equality: {"fieldName": "value"}
     * - IN operator: {"fieldName": ["value1", "value2"]}
     * - Regex: {"fieldName": {"regex": "pattern"}}
     * - Comparisons: {"fieldName": {"gt": 10, "lte": 100}}
     * All criteria use AND logic.
     */
    private Map<String, Object> criteria;
    
    /**
     * Alias for criteria - JSON attribute conditions map.
     */
    private Map<String, Object> attributes;
    
    /**
     * Legacy: List of attribute conditions for JSON search (v1.7.6 and earlier).
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
     * Check if API key is present.
     */
    public boolean hasApiKey() {
        return apiKey != null && !apiKey.isEmpty();
    }
    
    /**
     * Check if this is a simple single key lookup.
     */
    public boolean isKeyLookup() {
        return key != null && !key.isEmpty();
    }
    
    /**
     * Check if this is a multi-key lookup.
     */
    public boolean isMultiKeyLookup() {
        return keys != null && !keys.isEmpty();
    }
    
    /**
     * Check if this is a single key pattern (regex) search.
     */
    public boolean isKeyPatternSearch() {
        return keyPattern != null && !keyPattern.isEmpty();
    }
    
    /**
     * Check if this is a multi-pattern (regex) search.
     */
    public boolean isMultiKeyPatternSearch() {
        return keyPatterns != null && !keyPatterns.isEmpty();
    }
    
    /**
     * Check if this is a JSON criteria search (new format with criteria/attributes map).
     */
    public boolean isJsonCriteriaSearch() {
        if (!"json".equalsIgnoreCase(type)) return false;
        return (criteria != null && !criteria.isEmpty()) || 
               (attributes != null && !attributes.isEmpty());
    }
    
    /**
     * Get criteria map (prefers 'criteria' field, falls back to 'attributes').
     */
    public Map<String, Object> getCriteria() {
        if (criteria != null && !criteria.isEmpty()) {
            return criteria;
        }
        return attributes;
    }
    
    /**
     * Check if this is a legacy JSON search (old format with values list).
     */
    public boolean isLegacyJsonSearch() {
        return "json".equalsIgnoreCase(type) && values != null && !values.isEmpty();
    }
    
    /**
     * Check if this is any form of JSON search.
     */
    public boolean isJsonSearch() {
        return isJsonCriteriaSearch() || isLegacyJsonSearch();
    }
    
    /**
     * Check if any key-based search is specified.
     */
    public boolean hasKeySearch() {
        return isKeyLookup() || isMultiKeyLookup() || isKeyPatternSearch() || isMultiKeyPatternSearch();
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
