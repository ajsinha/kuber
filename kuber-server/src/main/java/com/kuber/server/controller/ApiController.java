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
import com.kuber.server.replication.ReplicationManager;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

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
    
    @GetMapping("/regions/{name}/stats")
    public ResponseEntity<Map<String, Object>> getRegionStats(@PathVariable String name) {
        return ResponseEntity.ok(cacheService.getStatistics(name));
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
