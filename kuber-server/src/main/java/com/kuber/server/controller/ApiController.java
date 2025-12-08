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
package com.kuber.server.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.dto.GenericSearchRequest;
import com.kuber.server.replication.ReplicationManager;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * REST API controller for programmatic cache access.
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ApiController {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // ==================== Server Info ====================
    
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getInfo() {
        Map<String, Object> info = cacheService.getServerInfo();
        if (replicationManager != null) {
            info.put("replication", replicationManager.getReplicationInfo());
        }
        return ResponseEntity.ok(info);
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("nodeId", properties.getNodeId());
        health.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        return ResponseEntity.ok(health);
    }
    
    // ==================== Region Operations ====================
    
    @GetMapping("/regions")
    public ResponseEntity<Collection<CacheRegion>> listRegions() {
        return ResponseEntity.ok(cacheService.getAllRegions());
    }
    
    @GetMapping("/regions/{name}")
    public ResponseEntity<CacheRegion> getRegion(@PathVariable String name) {
        CacheRegion region = cacheService.getRegion(name);
        if (region == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(region);
    }
    
    @PostMapping("/regions")
    public ResponseEntity<CacheRegion> createRegion(@RequestBody RegionRequest request) {
        CacheRegion region = cacheService.createRegion(request.getName(), request.getDescription());
        return ResponseEntity.ok(region);
    }
    
    @DeleteMapping("/regions/{name}")
    public ResponseEntity<Void> deleteRegion(@PathVariable String name) {
        cacheService.deleteRegion(name);
        return ResponseEntity.ok().build();
    }
    
    @PostMapping("/regions/{name}/purge")
    public ResponseEntity<Void> purgeRegion(@PathVariable String name) {
        cacheService.purgeRegion(name);
        return ResponseEntity.ok().build();
    }
    
    /**
     * Reload a region's value cache from persistence.
     * 
     * <p>This operation:
     * <ol>
     *   <li>Evicts all entries from the value cache (memory)</li>
     *   <li>Reloads entries from persistence up to max-memory-entries limit</li>
     * </ol>
     * 
     * <p>Use this when:
     * <ul>
     *   <li>Value cache may have stale data</li>
     *   <li>Memory needs refresh with current disk data</li>
     *   <li>After direct database modifications</li>
     * </ul>
     * 
     * @param name Region name
     * @return Map with reload statistics
     */
    @PostMapping("/regions/{name}/reload")
    public ResponseEntity<Map<String, Object>> reloadRegionFromPersistence(@PathVariable String name) {
        int entriesLoaded = cacheService.reloadRegionFromPersistence(name);
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("region", name);
        result.put("entriesLoaded", entriesLoaded);
        result.put("message", "Cache reloaded from persistence");
        result.put("timestamp", java.time.Instant.now().toString());
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Warm a region's cache by loading entries from persistence.
     * 
     * <p>Loads additional entries from persistence into memory without
     * clearing existing cache entries. Use after bulk operations.
     * 
     * @param name Region name
     * @return Map with warming statistics
     */
    @PostMapping("/regions/{name}/warm")
    public ResponseEntity<Map<String, Object>> warmRegionCache(@PathVariable String name) {
        int entriesWarmed = cacheService.warmRegionCache(name);
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("region", name);
        result.put("entriesWarmed", entriesWarmed);
        result.put("message", "Cache warmed from persistence");
        result.put("timestamp", java.time.Instant.now().toString());
        
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/regions/{name}/stats")
    public ResponseEntity<Map<String, Object>> getRegionStats(@PathVariable String name) {
        return ResponseEntity.ok(cacheService.getStatistics(name));
    }
    
    // ==================== Attribute Mapping Operations ====================
    
    /**
     * Get attribute mapping for a region.
     * Returns the current attribute mapping configuration.
     */
    @GetMapping("/regions/{name}/attributemapping")
    public ResponseEntity<Map<String, String>> getAttributeMapping(@PathVariable String name) {
        Map<String, String> mapping = cacheService.getAttributeMapping(name);
        return ResponseEntity.ok(mapping);
    }
    
    /**
     * Set attribute mapping for a region.
     * Maps source attribute names to target attribute names for JSON transformation.
     * When JSON is stored in this region, attributes are renamed according to this mapping.
     * 
     * Example request body:
     * {"firstName": "first_name", "lastName": "last_name", "emailAddress": "email"}
     */
    @PutMapping("/regions/{name}/attributemapping")
    public ResponseEntity<Void> setAttributeMapping(@PathVariable String name,
                                                     @RequestBody Map<String, String> mapping) {
        cacheService.setAttributeMapping(name, mapping);
        return ResponseEntity.ok().build();
    }
    
    /**
     * Clear attribute mapping for a region.
     */
    @DeleteMapping("/regions/{name}/attributemapping")
    public ResponseEntity<Void> clearAttributeMapping(@PathVariable String name) {
        cacheService.clearAttributeMapping(name);
        return ResponseEntity.ok().build();
    }
    
    // ==================== Key/Value Operations ====================
    
    @GetMapping("/cache/{region}/keys")
    public ResponseEntity<Set<String>> getKeys(@PathVariable String region,
                                               @RequestParam(defaultValue = "*") String pattern) {
        return ResponseEntity.ok(cacheService.keys(region, pattern));
    }
    
    /**
     * Search keys by regex pattern and return key-value pairs.
     * Different from /keys which uses glob pattern and returns only keys.
     */
    @GetMapping("/cache/{region}/ksearch")
    public ResponseEntity<List<Map<String, Object>>> searchKeysByRegex(
            @PathVariable String region,
            @RequestParam String pattern,
            @RequestParam(defaultValue = "1000") int limit) {
        return ResponseEntity.ok(cacheService.searchKeysByRegex(region, pattern, limit));
    }
    
    @GetMapping("/cache/{region}/{key}")
    public ResponseEntity<Map<String, Object>> getValue(@PathVariable String region,
                                                        @PathVariable String key) {
        String value = cacheService.get(region, key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("value", value);
        result.put("type", cacheService.type(region, key));
        result.put("ttl", cacheService.ttl(region, key));
        
        return ResponseEntity.ok(result);
    }
    
    @PutMapping("/cache/{region}/{key}")
    public ResponseEntity<Map<String, Object>> setValue(@PathVariable String region,
                                                        @PathVariable String key,
                                                        @RequestBody ValueRequest request) {
        long ttl = request.getTtl() != null ? request.getTtl() : -1;
        
        if (request.getJson() != null) {
            JsonNode json = JsonUtils.parse(JsonUtils.toJson(request.getJson()));
            cacheService.jsonSet(region, key, "$", json, ttl);
        } else {
            cacheService.set(region, key, request.getValue(), ttl);
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("status", "OK");
        
        return ResponseEntity.ok(result);
    }
    
    @DeleteMapping("/cache/{region}/{key}")
    public ResponseEntity<Map<String, Object>> deleteValue(@PathVariable String region,
                                                           @PathVariable String key) {
        boolean deleted = cacheService.delete(region, key);
        
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("deleted", deleted);
        
        return ResponseEntity.ok(result);
    }
    
    // ==================== JSON Operations ====================
    
    @GetMapping("/cache/{region}/{key}/json")
    public ResponseEntity<JsonNode> getJson(@PathVariable String region,
                                           @PathVariable String key,
                                           @RequestParam(defaultValue = "$") String path) {
        JsonNode json = cacheService.jsonGet(region, key, path);
        if (json == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(json);
    }
    
    @PutMapping("/cache/{region}/{key}/json")
    public ResponseEntity<Map<String, Object>> setJson(@PathVariable String region,
                                                       @PathVariable String key,
                                                       @RequestParam(defaultValue = "$") String path,
                                                       @RequestBody Object value) {
        JsonNode json = JsonUtils.parse(JsonUtils.toJson(value));
        long ttl = -1;
        cacheService.jsonSet(region, key, path, json, ttl);
        
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("path", path);
        result.put("status", "OK");
        
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/cache/{region}/search")
    public ResponseEntity<List<Map<String, Object>>> searchJson(@PathVariable String region,
                                                                @RequestBody SearchRequest request) {
        List<CacheEntry> entries = cacheService.jsonSearch(region, request.getQuery());
        
        List<Map<String, Object>> results = new ArrayList<>();
        for (CacheEntry entry : entries) {
            Map<String, Object> item = new HashMap<>();
            item.put("key", entry.getKey());
            item.put("value", entry.getJsonValue());
            results.add(item);
        }
        
        return ResponseEntity.ok(results);
    }
    
    // ==================== Generic Search API ====================
    
    /**
     * Generic search endpoint that supports multiple search modes:
     * 
     * 1. Simple key lookup: {"region": "test", "key": "ABC"}
     * 2. Key pattern (regex): {"region": "test", "keypattern": "ABC.*"}
     * 3. JSON attribute search: {"region": "test", "type": "json", "values": [...]}
     * 
     * JSON attribute conditions support:
     * - Equality: {"fieldName": "value"}
     * - Regex: {"fieldName": "pattern", "type": "regex"}
     * - IN operator: {"fieldName": ["value1", "value2"]}
     * 
     * @param request The search request
     * @return List of matching key-value pairs as JSON array
     */
    @PostMapping({"/genericsearch", "/v1/genericsearch"})
    public ResponseEntity<List<Map<String, Object>>> genericSearch(@RequestBody GenericSearchRequest request) {
        // Validate region
        if (request.getRegion() == null || request.getRegion().isEmpty()) {
            return ResponseEntity.badRequest().body(List.of(
                Map.of("error", "Region is required")
            ));
        }
        
        String region = request.getRegion();
        int limit = request.getLimitOrDefault();
        List<String> fields = request.getFields();
        List<Map<String, Object>> results = new ArrayList<>();
        
        try {
            if (request.isKeyLookup()) {
                // Mode 1: Simple key lookup
                String value = cacheService.get(region, request.getKey());
                if (value != null) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("key", request.getKey());
                    
                    // Try to parse as JSON
                    try {
                        JsonNode jsonValue = JsonUtils.parse(value);
                        // Apply field projection if requested
                        if (request.hasFieldProjection()) {
                            item.put("value", projectFields(jsonValue, fields));
                        } else {
                            item.put("value", jsonValue);
                        }
                    } catch (Exception e) {
                        item.put("value", value);
                    }
                    
                    results.add(item);
                }
            } else if (request.isKeyPatternSearch()) {
                // Mode 2: Key pattern (regex) search
                results = cacheService.searchKeysByRegex(region, request.getKeyPattern(), limit);
                // Apply field projection if requested
                if (request.hasFieldProjection()) {
                    results = applyFieldProjection(results, fields);
                }
            } else if (request.isJsonSearch()) {
                // Mode 3: JSON attribute search
                results = performJsonAttributeSearch(region, request.getValues(), limit, fields);
            } else {
                return ResponseEntity.badRequest().body(List.of(
                    Map.of("error", "Invalid search request. Provide 'key', 'keypattern', or 'type'='json' with 'values'")
                ));
            }
            
            return ResponseEntity.ok(results);
            
        } catch (PatternSyntaxException e) {
            return ResponseEntity.badRequest().body(List.of(
                Map.of("error", "Invalid regex pattern: " + e.getMessage())
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(List.of(
                Map.of("error", "Search failed: " + e.getMessage())
            ));
        }
    }
    
    /**
     * Apply field projection to a list of results.
     */
    private List<Map<String, Object>> applyFieldProjection(List<Map<String, Object>> results, List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return results;
        }
        
        List<Map<String, Object>> projected = new ArrayList<>();
        for (Map<String, Object> result : results) {
            Map<String, Object> item = new HashMap<>();
            item.put("key", result.get("key"));
            
            Object value = result.get("value");
            if (value instanceof JsonNode) {
                item.put("value", projectFields((JsonNode) value, fields));
            } else if (value instanceof Map) {
                item.put("value", projectFieldsFromMap((Map<String, Object>) value, fields));
            } else if (value instanceof String) {
                // Try to parse as JSON
                try {
                    JsonNode jsonValue = JsonUtils.parse((String) value);
                    item.put("value", projectFields(jsonValue, fields));
                } catch (Exception e) {
                    item.put("value", value);
                }
            } else {
                item.put("value", value);
            }
            
            projected.add(item);
        }
        return projected;
    }
    
    /**
     * Project specified fields from a JsonNode.
     * Supports nested paths with dot notation (e.g., "address.city").
     */
    private Map<String, Object> projectFields(JsonNode json, List<String> fields) {
        if (json == null || !json.isObject()) {
            return Collections.emptyMap();
        }
        
        Map<String, Object> result = new LinkedHashMap<>();
        for (String fieldPath : fields) {
            JsonNode fieldValue = getJsonField(json, fieldPath);
            if (fieldValue != null && !fieldValue.isNull() && !fieldValue.isMissingNode()) {
                // Store with the full path as key to preserve nested structure info
                setNestedField(result, fieldPath, jsonNodeToObject(fieldValue));
            }
        }
        return result;
    }
    
    /**
     * Project specified fields from a Map.
     */
    private Map<String, Object> projectFieldsFromMap(Map<String, Object> map, List<String> fields) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String fieldPath : fields) {
            Object fieldValue = getMapField(map, fieldPath);
            if (fieldValue != null) {
                setNestedField(result, fieldPath, fieldValue);
            }
        }
        return result;
    }
    
    /**
     * Get a field value from a Map, supporting nested paths.
     */
    private Object getMapField(Map<String, Object> map, String fieldPath) {
        if (fieldPath.contains(".")) {
            String[] parts = fieldPath.split("\\.", 2);
            Object nested = map.get(parts[0]);
            if (nested instanceof Map) {
                return getMapField((Map<String, Object>) nested, parts[1]);
            }
            return null;
        }
        return map.get(fieldPath);
    }
    
    /**
     * Set a nested field value in a result map.
     * For "address.city", creates {"address": {"city": value}}
     */
    private void setNestedField(Map<String, Object> result, String fieldPath, Object value) {
        if (fieldPath.contains(".")) {
            String[] parts = fieldPath.split("\\.", 2);
            Map<String, Object> nested = (Map<String, Object>) result.computeIfAbsent(
                parts[0], k -> new LinkedHashMap<>());
            setNestedField(nested, parts[1], value);
        } else {
            result.put(fieldPath, value);
        }
    }
    
    /**
     * Convert JsonNode to appropriate Java object.
     */
    private Object jsonNodeToObject(JsonNode node) {
        if (node.isTextual()) {
            return node.asText();
        } else if (node.isNumber()) {
            if (node.isInt()) return node.asInt();
            if (node.isLong()) return node.asLong();
            return node.asDouble();
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isArray()) {
            List<Object> list = new ArrayList<>();
            node.forEach(item -> list.add(jsonNodeToObject(item)));
            return list;
        } else if (node.isObject()) {
            Map<String, Object> map = new LinkedHashMap<>();
            node.fields().forEachRemaining(e -> map.put(e.getKey(), jsonNodeToObject(e.getValue())));
            return map;
        }
        return node.asText();
    }
    
    /**
     * Perform JSON attribute search with support for equality, regex, and IN operators.
     */
    private List<Map<String, Object>> performJsonAttributeSearch(String region, 
                                                                  List<Map<String, Object>> conditions, 
                                                                  int limit,
                                                                  List<String> fields) {
        // Get all JSON entries in the region
        Set<String> keys = cacheService.keys(region, "*");
        List<Map<String, Object>> results = new ArrayList<>();
        
        for (String key : keys) {
            if (results.size() >= limit) break;
            
            JsonNode json = cacheService.jsonGet(region, key, "$");
            if (json == null) continue;
            
            // Check if all conditions match
            boolean allMatch = true;
            for (Map<String, Object> condition : conditions) {
                if (!matchesCondition(json, condition)) {
                    allMatch = false;
                    break;
                }
            }
            
            if (allMatch) {
                Map<String, Object> item = new HashMap<>();
                item.put("key", key);
                // Apply field projection if requested
                if (fields != null && !fields.isEmpty()) {
                    item.put("value", projectFields(json, fields));
                } else {
                    item.put("value", json);
                }
                results.add(item);
            }
        }
        
        return results;
    }
    
    /**
     * Check if a JSON document matches a single condition.
     * 
     * Condition formats:
     * - Equality: {"fieldName": "value"}
     * - Regex: {"fieldName": "pattern", "type": "regex"}
     * - IN: {"fieldName": ["value1", "value2"]}
     */
    private boolean matchesCondition(JsonNode json, Map<String, Object> condition) {
        // Check for 'type' key to determine match type
        String matchType = "equals"; // default
        if (condition.containsKey("type")) {
            Object typeValue = condition.get("type");
            if ("regex".equalsIgnoreCase(String.valueOf(typeValue))) {
                matchType = "regex";
            }
        }
        
        // Find the field name (skip 'type' key)
        for (Map.Entry<String, Object> entry : condition.entrySet()) {
            String fieldName = entry.getKey();
            Object conditionValue = entry.getValue();
            
            // Skip the 'type' key
            if ("type".equals(fieldName)) continue;
            
            // Get the field value from JSON (support nested paths with dot notation)
            JsonNode fieldValue = getJsonField(json, fieldName);
            
            if (fieldValue == null || fieldValue.isNull() || fieldValue.isMissingNode()) {
                return false;
            }
            
            // Handle different condition types
            if (conditionValue instanceof List) {
                // IN operator: value must be in the list
                List<?> valueList = (List<?>) conditionValue;
                String actualValue = fieldValue.asText();
                boolean found = valueList.stream()
                    .map(String::valueOf)
                    .anyMatch(v -> v.equals(actualValue));
                if (!found) return false;
            } else if ("regex".equals(matchType)) {
                // Regex match
                String pattern = String.valueOf(conditionValue);
                String actualValue = fieldValue.asText();
                if (!Pattern.matches(pattern, actualValue)) {
                    return false;
                }
            } else {
                // Equality match
                String expectedValue = String.valueOf(conditionValue);
                String actualValue = fieldValue.asText();
                if (!expectedValue.equals(actualValue)) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    /**
     * Get a field value from JSON, supporting nested paths with dot notation.
     */
    private JsonNode getJsonField(JsonNode json, String fieldPath) {
        if (fieldPath.contains(".")) {
            // Nested path
            String[] parts = fieldPath.split("\\.");
            JsonNode current = json;
            for (String part : parts) {
                if (current == null || !current.has(part)) {
                    return null;
                }
                current = current.get(part);
            }
            return current;
        } else {
            // Simple field
            return json.get(fieldPath);
        }
    }
    
    // ==================== Hash Operations ====================
    
    @GetMapping("/cache/{region}/{key}/hash")
    public ResponseEntity<Map<String, String>> getHash(@PathVariable String region,
                                                       @PathVariable String key) {
        Map<String, String> hash = cacheService.hgetall(region, key);
        return ResponseEntity.ok(hash);
    }
    
    @GetMapping("/cache/{region}/{key}/hash/{field}")
    public ResponseEntity<Map<String, Object>> getHashField(@PathVariable String region,
                                                            @PathVariable String key,
                                                            @PathVariable String field) {
        String value = cacheService.hget(region, key, field);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("field", field);
        result.put("value", value);
        
        return ResponseEntity.ok(result);
    }
    
    @PutMapping("/cache/{region}/{key}/hash/{field}")
    public ResponseEntity<Map<String, Object>> setHashField(@PathVariable String region,
                                                            @PathVariable String key,
                                                            @PathVariable String field,
                                                            @RequestBody ValueRequest request) {
        cacheService.hset(region, key, field, request.getValue());
        
        Map<String, Object> result = new HashMap<>();
        result.put("field", field);
        result.put("status", "OK");
        
        return ResponseEntity.ok(result);
    }
    
    // ==================== Batch Operations ====================
    
    @PostMapping("/cache/{region}/mget")
    public ResponseEntity<Map<String, String>> mget(@PathVariable String region,
                                                    @RequestBody List<String> keys) {
        List<String> values = cacheService.mget(region, keys);
        
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), values.get(i));
        }
        
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/cache/{region}/mset")
    public ResponseEntity<Map<String, Object>> mset(@PathVariable String region,
                                                    @RequestBody Map<String, String> entries) {
        cacheService.mset(region, entries);
        
        Map<String, Object> result = new HashMap<>();
        result.put("count", entries.size());
        result.put("status", "OK");
        
        return ResponseEntity.ok(result);
    }
    
    // ==================== TTL Operations ====================
    
    @PostMapping("/cache/{region}/{key}/expire")
    public ResponseEntity<Map<String, Object>> setExpire(@PathVariable String region,
                                                         @PathVariable String key,
                                                         @RequestParam long seconds) {
        boolean result = cacheService.expire(region, key, seconds);
        
        Map<String, Object> response = new HashMap<>();
        response.put("key", key);
        response.put("success", result);
        
        return ResponseEntity.ok(response);
    }
    
    @DeleteMapping("/cache/{region}/{key}/expire")
    public ResponseEntity<Map<String, Object>> persist(@PathVariable String region,
                                                       @PathVariable String key) {
        boolean result = cacheService.persist(region, key);
        
        Map<String, Object> response = new HashMap<>();
        response.put("key", key);
        response.put("success", result);
        
        return ResponseEntity.ok(response);
    }
    
    // ==================== Request/Response DTOs ====================
    
    @Data
    public static class RegionRequest {
        private String name;
        private String description;
    }
    
    @Data
    public static class ValueRequest {
        private String value;
        private Object json;
        private Long ttl;
    }
    
    @Data
    public static class SearchRequest {
        private String query;
    }
}
