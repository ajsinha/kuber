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
import com.kuber.server.dto.GenericUpdateRequest;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.search.ParallelJsonSearchService;
import com.kuber.server.security.ApiKeyService;
import com.kuber.server.security.AuthorizationService;
import com.kuber.server.util.SlashKeyResolver;
import jakarta.servlet.http.HttpServletRequest;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * REST API controller for programmatic cache access.
 * Enforces RBAC permissions for all operations.
 * 
 * @version 2.4.0
 */
@RestController
@RequestMapping({"/api", "/api/v1"})
@RequiredArgsConstructor
public class ApiController {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final AuthorizationService authorizationService;
    private final ApiKeyService apiKeyService;
    private final ParallelJsonSearchService parallelJsonSearchService;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // ==================== Server Info ====================
    
    @GetMapping("/ping")
    public ResponseEntity<Map<String, Object>> ping() {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "OK");
        result.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getInfo() {
        Map<String, Object> info = cacheService.getServerInfo();
        if (replicationManager != null) {
            info.put("replication", replicationManager.getReplicationInfo());
        }
        return ResponseEntity.ok(info);
    }
    
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "UP");
        status.put("nodeId", properties.getNodeId());
        status.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        status.put("version", properties.getVersion());
        return ResponseEntity.ok(status);
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("nodeId", properties.getNodeId());
        health.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        return ResponseEntity.ok(health);
    }
    
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = cacheService.getServerInfo();
        stats.put("searchStats", parallelJsonSearchService.getStatistics());
        return ResponseEntity.ok(stats);
    }
    
    /**
     * Get search statistics including parallel search metrics.
     * @since 1.7.9
     */
    @GetMapping("/search/stats")
    public ResponseEntity<Map<String, Object>> getSearchStats() {
        return ResponseEntity.ok(parallelJsonSearchService.getStatistics());
    }
    
    // ==================== Region Operations ====================
    
    @GetMapping("/regions")
    public ResponseEntity<?> listRegions() {
        // Filter to accessible regions
        Collection<CacheRegion> allRegions = cacheService.getAllRegions();
        if (authorizationService.isAdmin()) {
            return ResponseEntity.ok(allRegions);
        }
        
        Collection<CacheRegion> accessible = allRegions.stream()
                .filter(r -> authorizationService.canRead(r.getName()) ||
                             authorizationService.canWrite(r.getName()) ||
                             authorizationService.canDelete(r.getName()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(accessible);
    }
    
    @GetMapping("/regions/{name}")
    public ResponseEntity<?> getRegion(@PathVariable String name) {
        // Check if user has any access to this region
        if (!authorizationService.isAdmin() && 
            !authorizationService.canRead(name) && 
            !authorizationService.canWrite(name) && 
            !authorizationService.canDelete(name)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "Access denied to region: " + name));
        }
        
        CacheRegion region = cacheService.getRegion(name);
        if (region == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(region);
    }
    
    @PostMapping("/regions")
    public ResponseEntity<?> createRegion(@RequestBody RegionRequest request) {
        // Only admin can create regions
        if (!authorizationService.isAdmin()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "Admin permission required to create regions"));
        }
        CacheRegion region = cacheService.createRegion(request.getName(), request.getDescription());
        return ResponseEntity.ok(region);
    }
    
    @DeleteMapping("/regions/{name}")
    public ResponseEntity<?> deleteRegion(@PathVariable String name) {
        // Only admin can delete regions
        if (!authorizationService.isAdmin()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "Admin permission required to delete regions"));
        }
        cacheService.deleteRegion(name);
        return ResponseEntity.ok().build();
    }
    
    @PostMapping("/regions/{name}/purge")
    public ResponseEntity<?> purgeRegion(@PathVariable String name) {
        // Require DELETE permission to purge
        if (!authorizationService.canDelete(name)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "DELETE permission denied for region: " + name));
        }
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
    public ResponseEntity<?> reloadRegionFromPersistence(@PathVariable String name) {
        // Require WRITE permission to reload
        if (!authorizationService.canWrite(name)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + name));
        }
        
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
    public ResponseEntity<?> warmRegionCache(@PathVariable String name) {
        // Require WRITE permission to warm
        if (!authorizationService.canWrite(name)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + name));
        }
        
        int entriesWarmed = cacheService.warmRegionCache(name);
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("region", name);
        result.put("entriesWarmed", entriesWarmed);
        result.put("message", "Cache warmed from persistence");
        result.put("timestamp", java.time.Instant.now().toString());
        
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/regions/{name}/stats")
    public ResponseEntity<?> getRegionStats(@PathVariable String name) {
        // Require READ permission to view stats
        if (!authorizationService.canRead(name)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + name));
        }
        return ResponseEntity.ok(cacheService.getStatistics(name));
    }
    
    // ==================== Attribute Mapping Operations ====================
    
    /**
     * Get attribute mapping for a region.
     * Returns the current attribute mapping configuration.
     */
    @GetMapping("/regions/{name}/attributemapping")
    public ResponseEntity<?> getAttributeMapping(@PathVariable String name) {
        // Require READ permission
        if (!authorizationService.canRead(name)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + name));
        }
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
    public ResponseEntity<?> setAttributeMapping(@PathVariable String name,
                                                     @RequestBody Map<String, String> mapping) {
        // Require admin permission to modify attribute mapping
        if (!authorizationService.isAdmin()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "Admin permission required to modify attribute mapping"));
        }
        cacheService.setAttributeMapping(name, mapping);
        return ResponseEntity.ok().build();
    }
    
    /**
     * Clear attribute mapping for a region.
     */
    @DeleteMapping("/regions/{name}/attributemapping")
    public ResponseEntity<?> clearAttributeMapping(@PathVariable String name) {
        // Require admin permission to clear attribute mapping
        if (!authorizationService.isAdmin()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "Admin permission required to clear attribute mapping"));
        }
        cacheService.clearAttributeMapping(name);
        return ResponseEntity.ok().build();
    }
    
    // ==================== Key/Value Operations ====================
    
    @GetMapping("/cache/{region}/keys")
    public ResponseEntity<?> getKeys(@PathVariable String region,
                                               @RequestParam(defaultValue = "*") String pattern) {
        // Require READ permission
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        return ResponseEntity.ok(cacheService.keys(region, pattern));
    }
    
    /**
     * Get keys matching a regex pattern (keys only, no values).
     * More efficient than /ksearch when you only need key names.
     * 
     * @param region the cache region
     * @param pattern Java regex pattern (e.g., "^user:\\d+$")
     * @param limit maximum number of keys to return (default: 1000)
     * @return list of matching keys
     */
    @GetMapping("/cache/{region}/keys/regex")
    public ResponseEntity<?> getKeysByRegex(
            @PathVariable String region,
            @RequestParam String pattern,
            @RequestParam(defaultValue = "1000") int limit) {
        // Require READ permission
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        return ResponseEntity.ok(cacheService.keysByRegex(region, pattern, limit));
    }
    
    /**
     * Search keys by regex pattern and return key-value pairs.
     * Different from /keys which uses glob pattern and returns only keys.
     */
    @GetMapping("/cache/{region}/ksearch")
    public ResponseEntity<?> searchKeysByRegex(
            @PathVariable String region,
            @RequestParam String pattern,
            @RequestParam(defaultValue = "1000") int limit) {
        // Require READ permission
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        return ResponseEntity.ok(cacheService.searchKeysByRegex(region, pattern, limit));
    }
    
    @GetMapping("/cache/{region}/{key}")
    public ResponseEntity<?> getValue(@PathVariable String region,
                                                        @PathVariable String key) {
        // Require READ permission
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
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
    public ResponseEntity<?> setValue(@PathVariable String region,
                                                        @PathVariable String key,
                                                        @RequestBody ValueRequest request) {
        // Require WRITE permission
        if (!authorizationService.canWrite(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + region));
        }
        
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
    public ResponseEntity<?> deleteValue(@PathVariable String region,
                                                           @PathVariable String key) {
        // Require DELETE permission
        if (!authorizationService.canDelete(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "DELETE permission denied for region: " + region));
        }
        
        boolean deleted = cacheService.delete(region, key);
        
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("deleted", deleted);
        
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/cache/{region}/{key}/exists")
    public ResponseEntity<Map<String, Object>> keyExists(@PathVariable String region,
                                                          @PathVariable String key) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        boolean exists = cacheService.exists(region, key);
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("exists", exists);
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/cache/{region}/{key}/ttl")
    public ResponseEntity<Map<String, Object>> getKeyTtl(@PathVariable String region,
                                                          @PathVariable String key) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        long ttl = cacheService.ttl(region, key);
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("ttl", ttl);
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/cache/{region}/size")
    public ResponseEntity<Map<String, Object>> getRegionSize(@PathVariable String region) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        Set<String> allKeys = cacheService.keys(region, "*", Integer.MAX_VALUE);
        Map<String, Object> result = new HashMap<>();
        result.put("region", region);
        result.put("size", allKeys.size());
        return ResponseEntity.ok(result);
    }
    
    // ==================== JSON Operations ====================
    
    @GetMapping("/cache/{region}/{key}/json")
    public ResponseEntity<?> getJson(@PathVariable String region,
                                           @PathVariable String key,
                                           @RequestParam(defaultValue = "$") String path) {
        // Require READ permission
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        JsonNode json = cacheService.jsonGet(region, key, path);
        if (json == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(json);
    }
    
    @PutMapping("/cache/{region}/{key}/json")
    public ResponseEntity<?> setJson(@PathVariable String region,
                                                       @PathVariable String key,
                                                       @RequestParam(defaultValue = "$") String path,
                                                       @RequestBody Object value) {
        // Require WRITE permission
        if (!authorizationService.canWrite(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + region));
        }
        
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
    public ResponseEntity<?> searchJson(@PathVariable String region,
                                                                @RequestBody SearchRequest request) {
        // Require READ permission
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
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
    
    // ==================== JSON Alternative Paths ====================
    
    @GetMapping("/json/{region}/{key}")
    public ResponseEntity<?> getJsonAlt(@PathVariable String region,
                                         @PathVariable String key,
                                         @RequestParam(defaultValue = "$") String path) {
        return getJson(region, key, path);
    }
    
    @PutMapping("/json/{region}/{key}")
    public ResponseEntity<?> setJsonAlt(@PathVariable String region,
                                         @PathVariable String key,
                                         @RequestBody Map<String, Object> body) {
        if (!authorizationService.canWrite(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + region));
        }
        
        Object value = body.get("value");
        Long ttlValue = body.containsKey("ttl") ? ((Number) body.get("ttl")).longValue() : -1L;
        
        JsonNode json = JsonUtils.parse(JsonUtils.toJson(value));
        cacheService.jsonSet(region, key, "$", json, ttlValue);
        
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("status", "OK");
        return ResponseEntity.ok(result);
    }
    
    @DeleteMapping("/json/{region}/{key}")
    public ResponseEntity<?> deleteJsonAlt(@PathVariable String region,
                                            @PathVariable String key,
                                            @RequestParam(defaultValue = "$") String path) {
        if (!authorizationService.canDelete(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "DELETE permission denied for region: " + region));
        }
        
        boolean deleted = cacheService.jsonDelete(region, key, path);
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("deleted", deleted);
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/json/{region}/search")
    public ResponseEntity<?> searchJsonAlt(@PathVariable String region,
                                            @RequestBody SearchRequest request) {
        return searchJson(region, request);
    }
    
    // ==================== Slash-Key Fallback Endpoints ====================
    //
    // These /**-pattern endpoints handle cache keys that contain forward slashes
    // (e.g. "employee/EMP001"). When clients URL-encode the slash as %2F, and the
    // web container decodes it back to '/' before Spring's pattern matching, the
    // standard {key} pattern fails (too many path segments). These catch-all patterns
    // reassemble the full key from the remaining path, handling both encoded and
    // decoded slashes transparently.
    //
    // Spring's AntPathMatcher prefers more-specific patterns, so simple keys still
    // match the {key} endpoints above; these /** variants only activate when the
    // decoded path has extra segments.
    
    @GetMapping("/json/{region}/**")
    public ResponseEntity<?> getJsonSlashKey(@PathVariable String region,
                                              @RequestParam(defaultValue = "$") String path,
                                              HttpServletRequest request) {
        String key = SlashKeyResolver.resolveKey(request);
        if (key == null || key.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Key is required"));
        }
        return getJson(region, key, path);
    }
    
    @PutMapping("/json/{region}/**")
    public ResponseEntity<?> setJsonSlashKey(@PathVariable String region,
                                              @RequestBody Map<String, Object> body,
                                              HttpServletRequest request) {
        String key = SlashKeyResolver.resolveKey(request);
        if (key == null || key.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Key is required"));
        }
        if (!authorizationService.canWrite(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + region));
        }
        Object value = body.get("value");
        Long ttlValue = body.containsKey("ttl") ? ((Number) body.get("ttl")).longValue() : -1L;
        JsonNode json = JsonUtils.parse(JsonUtils.toJson(value));
        cacheService.jsonSet(region, key, "$", json, ttlValue);
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("status", "OK");
        return ResponseEntity.ok(result);
    }
    
    @DeleteMapping("/json/{region}/**")
    public ResponseEntity<?> deleteJsonSlashKey(@PathVariable String region,
                                                 @RequestParam(defaultValue = "$") String path,
                                                 HttpServletRequest request) {
        String key = SlashKeyResolver.resolveKey(request);
        if (key == null || key.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Key is required"));
        }
        if (!authorizationService.canDelete(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "DELETE permission denied for region: " + region));
        }
        boolean deleted = cacheService.jsonDelete(region, key, path);
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("deleted", deleted);
        return ResponseEntity.ok(result);
    }
    
    @DeleteMapping("/cache/{region}/{key}/json")
    public ResponseEntity<?> deleteJson(@PathVariable String region,
                                         @PathVariable String key,
                                         @RequestParam(defaultValue = "$") String path) {
        if (!authorizationService.canDelete(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "DELETE permission denied for region: " + region));
        }
        
        boolean deleted = cacheService.jsonDelete(region, key, path);
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("deleted", deleted);
        return ResponseEntity.ok(result);
    }
    
    // ==================== Bulk Operations ====================
    
    @PostMapping("/cache/{region}/import")
    public ResponseEntity<?> bulkImport(@PathVariable String region,
                                         @RequestBody Map<String, Object> body) {
        if (!authorizationService.canWrite(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + region));
        }
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entries = (List<Map<String, Object>>) body.get("entries");
        int imported = 0;
        if (entries != null) {
            for (Map<String, Object> entry : entries) {
                String key = (String) entry.get("key");
                Object value = entry.get("value");
                long ttl = entry.containsKey("ttl") ? ((Number) entry.get("ttl")).longValue() : -1L;
                
                if (value instanceof Map || value instanceof List) {
                    JsonNode json = JsonUtils.parse(JsonUtils.toJson(value));
                    cacheService.jsonSet(region, key, "$", json, ttl);
                } else {
                    cacheService.set(region, key, String.valueOf(value), ttl);
                }
                imported++;
            }
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("imported", imported);
        result.put("status", "OK");
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/cache/{region}/export")
    public ResponseEntity<?> bulkExport(@PathVariable String region,
                                         @RequestParam(defaultValue = "*") String pattern) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        Set<String> keys = cacheService.keys(region, pattern, 10000);
        List<Map<String, Object>> entries = new ArrayList<>();
        for (String key : keys) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("key", key);
            String value = cacheService.get(region, key);
            entry.put("value", value);
            entry.put("ttl", cacheService.ttl(region, key));
            entries.add(entry);
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("entries", entries);
        result.put("count", entries.size());
        return ResponseEntity.ok(result);
    }
    
    // ==================== Generic Search API (v1.7.9 Enhanced) ====================
    
    /**
     * Enhanced generic search endpoint (v1.7.9) supporting multiple search modes.
     * 
     * <h2>Authentication</h2>
     * API key must be provided in the request body:
     * <pre>{"apiKey": "your-api-key", ...}</pre>
     * 
     * <h2>Search Modes</h2>
     * 
     * 1. Single key lookup: {"apiKey": "xxx", "region": "test", "key": "ABC"}
     * 2. Multiple keys lookup: {"apiKey": "xxx", "region": "test", "keys": ["A", "B", "C"]}
     * 3. Single key pattern (regex): {"apiKey": "xxx", "region": "test", "keyPattern": "user:.*"}
     * 4. Multiple key patterns: {"apiKey": "xxx", "region": "test", "keyPatterns": ["user:.*", "admin:.*"]}
     * 5. JSON attribute search with criteria (AND logic):
     *    {
     *      "apiKey": "xxx",
     *      "region": "test",
     *      "type": "json",
     *      "criteria": {
     *        "status": "active",                          // equality
     *        "country": ["USA", "Canada"],                // IN operator
     *        "email": {"regex": ".*@company\\.com"},      // regex match
     *        "age": {"gte": 18, "lte": 65}                // range comparison
     *      }
     *    }
     * 
     * @param request The search request with apiKey
     * @return List of matching key-value pairs as JSON array
     */
    @PostMapping({"/genericsearch", "/v1/genericsearch", "/v2/genericsearch"})
    public ResponseEntity<List<Map<String, Object>>> genericSearch(@RequestBody GenericSearchRequest request) {
        // Validate API key
        if (!request.hasApiKey()) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(List.of(
                Map.of("error", "API key is required. Include 'apiKey' in request body.")
            ));
        }
        
        // Validate API key against configured keys
        if (!isValidApiKey(request.getApiKey())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(List.of(
                Map.of("error", "Invalid API key")
            ));
        }
        
        // Validate region
        if (request.getRegion() == null || request.getRegion().isEmpty()) {
            return ResponseEntity.badRequest().body(List.of(
                Map.of("error", "Region is required")
            ));
        }
        
        String region = request.getRegion();
        
        // Check if region exists
        if (!cacheService.regionExists(region)) {
            return ResponseEntity.badRequest().body(List.of(
                Map.of("error", "Region does not exist: " + region)
            ));
        }
        
        int limit = request.getLimitOrDefault();
        List<String> fields = request.getFields();
        List<Map<String, Object>> results = new ArrayList<>();
        
        try {
            if (request.isKeyLookup()) {
                // Mode 1: Single key lookup
                results = performSingleKeyLookup(region, request.getKey(), fields);
                
            } else if (request.isMultiKeyLookup()) {
                // Mode 2: Multiple keys lookup (v1.7.9)
                results = performMultiKeyLookup(region, request.getKeys(), limit, fields);
                
            } else if (request.isKeyPatternSearch()) {
                // Mode 3: Single key pattern (regex) search
                results = performKeyPatternSearch(region, request.getKeyPattern(), limit, fields);
                
            } else if (request.isMultiKeyPatternSearch()) {
                // Mode 4: Multiple key patterns search (v1.7.9)
                results = performMultiKeyPatternSearch(region, request.getKeyPatterns(), limit, fields);
                
            } else if (request.isJsonCriteriaSearch()) {
                // Mode 5: JSON attribute search with new criteria format (v1.7.9)
                results = performJsonCriteriaSearch(region, request.getCriteria(), limit, fields);
                
            } else if (request.isJsonSearch()) {
                // Mode 5 (legacy): JSON attribute search with values format
                results = performJsonAttributeSearch(region, request.getValues(), limit, fields);
                
            } else {
                return ResponseEntity.badRequest().body(List.of(
                    Map.of("error", "Invalid search request. Provide one of: 'key', 'keys', 'keyPattern', 'keyPatterns', or 'type'='json' with 'criteria'/'values'")
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
     * Validate API key using ApiKeyService.
     */
    private boolean isValidApiKey(String apiKey) {
        if (apiKey == null || apiKey.isEmpty()) {
            return false;
        }
        // Use ApiKeyService for validation (without updating lastUsedAt for performance)
        return apiKeyService.validateKeyOnly(apiKey).isPresent();
    }
    
    /**
     * Mode 1: Single key lookup.
     */
    private List<Map<String, Object>> performSingleKeyLookup(String region, String key, List<String> fields) {
        List<Map<String, Object>> results = new ArrayList<>();
        String value = cacheService.get(region, key);
        if (value != null) {
            results.add(buildResultItem(key, value, fields));
        }
        return results;
    }
    
    /**
     * Mode 2: Multiple keys lookup (v1.7.9).
     */
    private List<Map<String, Object>> performMultiKeyLookup(String region, List<String> keys, int limit, List<String> fields) {
        List<Map<String, Object>> results = new ArrayList<>();
        for (String key : keys) {
            if (results.size() >= limit) break;
            String value = cacheService.get(region, key);
            if (value != null) {
                results.add(buildResultItem(key, value, fields));
            }
        }
        return results;
    }
    
    /**
     * Mode 3: Single key pattern (regex) search.
     * Uses parallel search for improved performance on large datasets.
     * 
     * @since 1.7.9 - Now uses parallel pattern search
     */
    private List<Map<String, Object>> performKeyPatternSearch(String region, String pattern, int limit, List<String> fields) {
        // Delegate to ParallelJsonSearchService for optimized pattern search
        return parallelJsonSearchService.searchByPattern(region, pattern, fields, limit);
    }
    
    /**
     * Mode 4: Multiple key patterns search (v1.7.9).
     * Returns keys matching ANY of the patterns (OR logic between patterns).
     * Uses parallel two-phase search for improved performance:
     * - Phase 1: Parallel key matching across partitions
     * - Phase 2: Parallel value fetching for matches
     * 
     * @since 1.7.9 - Now uses parallel pattern search
     */
    private List<Map<String, Object>> performMultiKeyPatternSearch(String region, List<String> patterns, int limit, List<String> fields) {
        // Delegate to ParallelJsonSearchService for optimized multi-pattern search
        return parallelJsonSearchService.searchByPatterns(region, patterns, fields, limit);
    }
    
    /**
     * Mode 5: JSON attribute search with criteria format.
     * Uses ParallelJsonSearchService for high-performance search with automatic
     * parallel/sequential mode selection based on dataset size.
     * Uses AND logic - all criteria must match.
     * 
     * @since 1.7.9 - Now uses parallel search for improved performance
     */
    private List<Map<String, Object>> performJsonCriteriaSearch(String region, Map<String, Object> criteria, int limit, List<String> fields) {
        // Delegate to ParallelJsonSearchService for optimized search
        return parallelJsonSearchService.search(region, criteria, fields, limit);
    }
    
    /**
     * Check if JSON document matches all criteria (AND logic).
     */
    private boolean matchesAllCriteria(JsonNode json, Map<String, Object> criteria) {
        for (Map.Entry<String, Object> entry : criteria.entrySet()) {
            String fieldPath = entry.getKey();
            Object criteriaValue = entry.getValue();
            
            JsonNode fieldValue = getJsonField(json, fieldPath);
            
            if (!matchesCriterion(fieldValue, criteriaValue)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Check if a field value matches a single criterion.
     * Supports: equality, list (IN), regex, and comparison operators.
     */
    @SuppressWarnings("unchecked")
    private boolean matchesCriterion(JsonNode fieldValue, Object criteriaValue) {
        if (fieldValue == null || fieldValue.isNull() || fieldValue.isMissingNode()) {
            return false;
        }
        
        String actualValue = fieldValue.asText();
        
        // Case 1: List of values (IN operator)
        if (criteriaValue instanceof List) {
            List<?> valueList = (List<?>) criteriaValue;
            return valueList.stream()
                .map(String::valueOf)
                .anyMatch(v -> v.equals(actualValue));
        }
        
        // Case 2: Map with operators (regex, comparison)
        if (criteriaValue instanceof Map) {
            Map<String, Object> operators = (Map<String, Object>) criteriaValue;
            
            // Regex operator
            if (operators.containsKey("regex")) {
                String pattern = String.valueOf(operators.get("regex"));
                return Pattern.matches(pattern, actualValue);
            }
            
            // Comparison operators for numeric values
            if (fieldValue.isNumber()) {
                double numValue = fieldValue.asDouble();
                
                if (operators.containsKey("gt")) {
                    double threshold = Double.parseDouble(String.valueOf(operators.get("gt")));
                    if (numValue <= threshold) return false;
                }
                if (operators.containsKey("gte")) {
                    double threshold = Double.parseDouble(String.valueOf(operators.get("gte")));
                    if (numValue < threshold) return false;
                }
                if (operators.containsKey("lt")) {
                    double threshold = Double.parseDouble(String.valueOf(operators.get("lt")));
                    if (numValue >= threshold) return false;
                }
                if (operators.containsKey("lte")) {
                    double threshold = Double.parseDouble(String.valueOf(operators.get("lte")));
                    if (numValue > threshold) return false;
                }
                if (operators.containsKey("ne")) {
                    double notEqual = Double.parseDouble(String.valueOf(operators.get("ne")));
                    if (numValue == notEqual) return false;
                }
                if (operators.containsKey("eq")) {
                    double equal = Double.parseDouble(String.valueOf(operators.get("eq")));
                    if (numValue != equal) return false;
                }
                return true;
            }
            
            // Not equals for strings
            if (operators.containsKey("ne")) {
                String notEqual = String.valueOf(operators.get("ne"));
                return !actualValue.equals(notEqual);
            }
            
            return false;
        }
        
        // Case 3: Simple equality
        String expectedValue = String.valueOf(criteriaValue);
        return actualValue.equals(expectedValue);
    }
    
    /**
     * Build a result item with optional field projection.
     */
    private Map<String, Object> buildResultItem(String key, String value, List<String> fields) {
        Map<String, Object> item = new HashMap<>();
        item.put("key", key);
        
        try {
            JsonNode jsonValue = JsonUtils.parse(value);
            if (fields != null && !fields.isEmpty()) {
                item.put("value", projectFields(jsonValue, fields));
            } else {
                item.put("value", jsonValue);
            }
        } catch (Exception e) {
            item.put("value", value);
        }
        
        return item;
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
    
    // ==================== Generic Update API (v1.7.9) ====================
    
    /**
     * Generic Update API - Unified SET/UPDATE operation via REST API.
     * 
     * Behavior:
     * 1. If key does NOT exist:
     *    - Creates new entry with given value
     *    - If type="json", stores as JSON data type
     *    - Applies TTL if provided, else -1 (no expiry)
     * 
     * 2. If key EXISTS:
     *    - If type != "json": Replaces value entirely
     *    - If type = "json": Performs JUPDATE (merge/update JSON fields)
     *
     * @param request the update request containing apiKey, region, key, value, type, ttl
     * @return response with status and resulting value
     */
    @PostMapping({"/genericupdate", "/v1/genericupdate", "/v2/genericupdate"})
    public ResponseEntity<Map<String, Object>> genericUpdate(@RequestBody GenericUpdateRequest request) {
        Map<String, Object> response = new LinkedHashMap<>();
        
        // Validate API key
        if (!request.hasApiKey()) {
            response.put("success", false);
            response.put("error", "API key is required. Include 'apiKey' in request body.");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
        }
        
        // Validate API key against configured keys
        if (!isValidApiKey(request.getApiKey())) {
            response.put("success", false);
            response.put("error", "Invalid API key");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
        }
        
        // Validate required fields
        String validationError = request.getValidationError();
        if (validationError != null) {
            response.put("success", false);
            response.put("error", validationError);
            return ResponseEntity.badRequest().body(response);
        }
        
        String region = request.getRegion();
        String key = request.getKey();
        Object value = request.getValue();
        boolean isJsonType = request.isJsonType();
        long ttl = request.getTtlOrDefault();
        
        // Check if region exists, create if not
        if (!cacheService.regionExists(region)) {
            try {
                cacheService.createRegion(region, "Auto-created by genericupdate API");
            } catch (Exception e) {
                response.put("success", false);
                response.put("error", "Failed to create region: " + e.getMessage());
                return ResponseEntity.badRequest().body(response);
            }
        }
        
        try {
            // Check if key exists
            boolean keyExists = cacheService.exists(region, key);
            String operation;
            Object resultValue;
            
            if (!keyExists) {
                // Key doesn't exist: SET the value
                operation = "created";
                if (isJsonType) {
                    // Store as JSON
                    JsonNode jsonValue = JsonUtils.getObjectMapper().valueToTree(value);
                    cacheService.jsonSet(region, key, "$", jsonValue, ttl);
                    resultValue = jsonValue;
                } else {
                    // Store as string
                    String stringValue = value instanceof String ? (String) value : JsonUtils.toJson(value);
                    cacheService.set(region, key, stringValue, ttl);
                    resultValue = stringValue;
                }
            } else {
                // Key exists
                if (!isJsonType) {
                    // Non-JSON: simple replacement
                    operation = "replaced";
                    String stringValue = value instanceof String ? (String) value : JsonUtils.toJson(value);
                    cacheService.set(region, key, stringValue, ttl > 0 ? ttl : -1);
                    resultValue = stringValue;
                } else {
                    // JSON type: use JUPDATE merge logic
                    operation = "merged";
                    JsonNode jsonValue = JsonUtils.getObjectMapper().valueToTree(value);
                    JsonNode mergedValue = cacheService.jsonUpdate(region, key, jsonValue, ttl > 0 ? ttl : -1);
                    resultValue = mergedValue;
                }
            }
            
            response.put("success", true);
            response.put("operation", operation);
            response.put("region", region);
            response.put("key", key);
            response.put("value", resultValue);
            if (ttl > 0) {
                response.put("ttl", ttl);
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", "Update failed: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
    
    // ==================== Hash Alternative Paths ====================
    
    @GetMapping("/hash/{region}/{key}")
    public ResponseEntity<Map<String, String>> getHashAlt(@PathVariable String region,
                                                          @PathVariable String key) {
        return getHash(region, key);
    }
    
    @GetMapping("/hash/{region}/{key}/{field}")
    public ResponseEntity<Map<String, Object>> getHashFieldAlt(@PathVariable String region,
                                                                @PathVariable String key,
                                                                @PathVariable String field) {
        return getHashField(region, key, field);
    }
    
    @PutMapping("/hash/{region}/{key}/{field}")
    public ResponseEntity<Map<String, Object>> setHashFieldAlt(@PathVariable String region,
                                                                @PathVariable String key,
                                                                @PathVariable String field,
                                                                @RequestBody ValueRequest request) {
        return setHashField(region, key, field, request);
    }
    
    @DeleteMapping("/hash/{region}/{key}/{field}")
    public ResponseEntity<Map<String, Object>> deleteHashFieldAlt(@PathVariable String region,
                                                                   @PathVariable String key,
                                                                   @PathVariable String field) {
        return deleteHashField(region, key, field);
    }
    
    @PostMapping("/hash/{region}/{key}/mset")
    public ResponseEntity<Map<String, Object>> setHashMultipleAlt(@PathVariable String region,
                                                                   @PathVariable String key,
                                                                   @RequestBody Map<String, Object> body) {
        return setHashMultiple(region, key, body);
    }
    
    @GetMapping("/hash/{region}/{key}/keys")
    public ResponseEntity<Map<String, Object>> getHashKeysAlt(@PathVariable String region,
                                                               @PathVariable String key) {
        return getHashKeys(region, key);
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
    
    @PutMapping("/cache/{region}/{key}/hash")
    public ResponseEntity<Map<String, Object>> setHashMultiple(@PathVariable String region,
                                                                @PathVariable String key,
                                                                @RequestBody Map<String, Object> body) {
        if (!authorizationService.canWrite(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + region));
        }
        
        @SuppressWarnings("unchecked")
        Map<String, String> fields = (Map<String, String>) body.get("fields");
        if (fields != null) {
            for (Map.Entry<String, String> entry : fields.entrySet()) {
                cacheService.hset(region, key, entry.getKey(), entry.getValue());
            }
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("status", "OK");
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/cache/{region}/{key}/hash/mget")
    public ResponseEntity<Map<String, Object>> hashMultiGet(@PathVariable String region,
                                                             @PathVariable String key,
                                                             @RequestBody Map<String, Object> body) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) body.get("fields");
        List<String> values = new ArrayList<>();
        if (fields != null) {
            for (String field : fields) {
                values.add(cacheService.hget(region, key, field));
            }
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("values", values);
        return ResponseEntity.ok(result);
    }
    
    @DeleteMapping("/cache/{region}/{key}/hash/{field}")
    public ResponseEntity<Map<String, Object>> deleteHashField(@PathVariable String region,
                                                                @PathVariable String key,
                                                                @PathVariable String field) {
        if (!authorizationService.canDelete(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "DELETE permission denied for region: " + region));
        }
        
        boolean deleted = cacheService.hdel(region, key, field);
        Map<String, Object> result = new HashMap<>();
        result.put("field", field);
        result.put("deleted", deleted);
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/cache/{region}/{key}/hash/keys")
    public ResponseEntity<Map<String, Object>> getHashKeys(@PathVariable String region,
                                                            @PathVariable String key) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        Set<String> keys = cacheService.hkeys(region, key);
        Map<String, Object> result = new HashMap<>();
        result.put("keys", new ArrayList<>(keys));
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/cache/{region}/{key}/hash/values")
    public ResponseEntity<Map<String, Object>> getHashValues(@PathVariable String region,
                                                              @PathVariable String key) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        Collection<String> values = cacheService.hvals(region, key);
        Map<String, Object> result = new HashMap<>();
        result.put("values", new ArrayList<>(values));
        return ResponseEntity.ok(result);
    }
    
    // ==================== Batch Operations ====================
    
    @PostMapping("/cache/{region}/mget")
    public ResponseEntity<?> mget(@PathVariable String region,
                                   @RequestBody Map<String, Object> body) {
        if (!authorizationService.canRead(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "READ permission denied for region: " + region));
        }
        
        @SuppressWarnings("unchecked")
        List<String> keys = (List<String>) body.get("keys");
        List<String> values = cacheService.mget(region, keys);
        
        Map<String, Object> result = new HashMap<>();
        result.put("values", values);
        
        Map<String, String> keyValueMap = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            keyValueMap.put(keys.get(i), values.get(i));
        }
        result.put("entries", keyValueMap);
        
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/cache/{region}/mset")
    public ResponseEntity<Map<String, Object>> mset(@PathVariable String region,
                                                    @RequestBody Map<String, Object> body) {
        if (!authorizationService.canWrite(region)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "WRITE permission denied for region: " + region));
        }
        
        @SuppressWarnings("unchecked")
        Object entriesObj = body.get("entries");
        Map<String, String> entries = new HashMap<>();
        
        if (entriesObj instanceof Map) {
            // Direct map format: {"entries": {"key1": "val1", "key2": "val2"}}
            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map<String, String>) entriesObj;
            entries = map;
        } else if (entriesObj instanceof List) {
            // List format: {"entries": [{"key": "k1", "value": "v1"}, ...]}
            @SuppressWarnings("unchecked")
            List<Map<String, String>> list = (List<Map<String, String>>) entriesObj;
            for (Map<String, String> item : list) {
                entries.put(item.get("key"), item.get("value"));
            }
        }
        
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
                                                         @RequestBody(required = false) Map<String, Object> body,
                                                         @RequestParam(required = false) Long seconds) {
        long ttlSeconds;
        if (seconds != null) {
            ttlSeconds = seconds;
        } else if (body != null) {
            Number ttlNum = (Number) body.getOrDefault("seconds", body.get("ttl"));
            ttlSeconds = ttlNum != null ? ttlNum.longValue() : 0;
        } else {
            ttlSeconds = 0;
        }
        
        boolean result = cacheService.expire(region, key, ttlSeconds);
        
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
