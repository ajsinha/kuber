/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 */
package com.kuber.server.controller;

import com.kuber.server.cache.CacheService;
import com.kuber.server.index.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Controller for secondary index management.
 * 
 * <p>Provides:
 * <ul>
 *   <li>REST API for index CRUD operations</li>
 *   <li>Admin UI page for index management</li>
 *   <li>Statistics and monitoring endpoints</li>
 * </ul>
 * 
 * @version 1.9.0
 * @since 1.9.0
 */
@Controller
@RequestMapping
@RequiredArgsConstructor
@Slf4j
public class IndexController {
    
    private final IndexManager indexManager;
    private final IndexConfiguration indexConfiguration;
    private final CacheService cacheService;
    
    // ==================== Admin UI ====================
    
    /**
     * Admin page for index management.
     */
    @GetMapping("/admin/indexes")
    @PreAuthorize("hasRole('ADMIN')")
    public String indexManagementPage(Model model) {
        model.addAttribute("statistics", indexManager.getStatistics());
        model.addAttribute("indexes", indexManager.listAllIndexes());
        model.addAttribute("config", indexConfiguration.getIndexingSettings());
        return "admin/indexes";
    }
    
    /**
     * Admin page for creating a new index.
     */
    @GetMapping("/admin/indexes/create")
    @PreAuthorize("hasRole('ADMIN')")
    public String createIndexPage(Model model) {
        // Get all available regions from cache service
        List<String> allRegions = cacheService.getAllRegions().stream()
                .map(r -> r.getName())
                .sorted()
                .collect(Collectors.toList());
        model.addAttribute("regions", allRegions);
        // Also provide regions that already have indexes for reference
        model.addAttribute("indexedRegions", indexManager.getIndexedRegions());
        return "admin/create-index";
    }
    
    /**
     * Admin page for indexes of a specific region.
     */
    @GetMapping("/admin/indexes/region/{region}")
    @PreAuthorize("hasRole('ADMIN')")
    public String regionIndexesPage(@PathVariable String region, Model model) {
        model.addAttribute("region", region);
        model.addAttribute("statistics", indexManager.getRegionStatistics(region));
        
        // Get indexes for this region only
        List<Map<String, Object>> regionIndexes = indexManager.listAllIndexes().stream()
            .filter(idx -> region.equals(idx.get("region")))
            .collect(Collectors.toList());
        model.addAttribute("indexes", regionIndexes);
        
        return "admin/region-indexes";
    }
    
    /**
     * Standalone stats page for a specific index.
     */
    @GetMapping("/admin/indexes/{region}/{field}/stats")
    @PreAuthorize("hasRole('ADMIN')")
    public String indexStatsPage(@PathVariable String region, @PathVariable String field, Model model) {
        model.addAttribute("region", region);
        model.addAttribute("field", field);
        
        SecondaryIndex index = indexManager.getIndex(region, field);
        if (index != null) {
            Map<String, Object> stats = index.getStatistics();
            stats.put("region", region);
            stats.put("lastRebuilt", index.getDefinition().getLastRebuiltAt());
            stats.put("description", index.getDefinition().getDescription());
            stats.put("isOffHeap", index.isOffHeap());
            stats.put("offHeapBytes", index.getOffHeapBytesUsed());
            model.addAttribute("stats", stats);
            model.addAttribute("found", true);
        } else {
            model.addAttribute("found", false);
        }
        
        return "admin/index-stats";
    }
    
    // ==================== REST API: Index Operations ====================
    
    /**
     * List all indexes.
     */
    @GetMapping("/api/admin/indexes")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> listIndexes() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("statistics", indexManager.getStatistics());
        response.put("indexes", indexManager.listAllIndexes());
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get indexes for a specific region.
     */
    @GetMapping("/api/admin/indexes/{region}")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getRegionIndexes(@PathVariable String region) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("region", region);
        response.put("statistics", indexManager.getRegionStatistics(region));
        return ResponseEntity.ok(response);
    }
    
    /**
     * Create a new index.
     */
    @PostMapping("/api/admin/indexes/{region}")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> createIndex(
            @PathVariable String region,
            @RequestBody CreateIndexRequest request) {
        
        Map<String, Object> response = new LinkedHashMap<>();
        
        if (request.getField() == null || request.getField().isBlank()) {
            response.put("success", false);
            response.put("error", "Field name is required");
            return ResponseEntity.badRequest().body(response);
        }
        
        IndexType type = IndexType.fromString(request.getType());
        
        boolean created = indexManager.createIndex(
            region, 
            request.getField().trim(), 
            type, 
            request.getDescription()
        );
        
        if (created) {
            // Optionally rebuild immediately
            if (request.isRebuildNow()) {
                indexManager.rebuildIndex(region, request.getField().trim());
            }
            
            response.put("success", true);
            response.put("message", "Index created successfully");
            response.put("region", region);
            response.put("field", request.getField());
            response.put("type", type.name());
            
            log.info("Index created via API: region={}, field={}, type={}", 
                region, request.getField(), type);
            
            return ResponseEntity.ok(response);
        } else {
            response.put("success", false);
            response.put("error", "Index already exists or maximum indexes reached");
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * Drop an index.
     */
    @DeleteMapping("/api/admin/indexes/{region}/{field}")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> dropIndex(
            @PathVariable String region,
            @PathVariable String field) {
        
        Map<String, Object> response = new LinkedHashMap<>();
        
        boolean dropped = indexManager.dropIndex(region, field);
        
        if (dropped) {
            response.put("success", true);
            response.put("message", "Index dropped successfully");
            response.put("region", region);
            response.put("field", field);
            
            log.info("Index dropped via API: region={}, field={}", region, field);
            
            return ResponseEntity.ok(response);
        } else {
            response.put("success", false);
            response.put("error", "Index not found");
            return ResponseEntity.notFound().build();
        }
    }
    
    /**
     * Rebuild an index.
     */
    @PostMapping("/api/admin/indexes/{region}/{field}/rebuild")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> rebuildIndex(
            @PathVariable String region,
            @PathVariable String field) {
        
        Map<String, Object> response = new LinkedHashMap<>();
        
        if (!indexManager.hasIndex(region, field)) {
            response.put("success", false);
            response.put("error", "Index not found");
            return ResponseEntity.notFound().build();
        }
        
        long startTime = System.currentTimeMillis();
        indexManager.rebuildIndex(region, field);
        long elapsed = System.currentTimeMillis() - startTime;
        
        SecondaryIndex index = indexManager.getIndex(region, field);
        
        response.put("success", true);
        response.put("message", "Index rebuilt successfully");
        response.put("region", region);
        response.put("field", field);
        response.put("entries", index != null ? index.size() : 0);
        response.put("rebuildTimeMs", elapsed);
        
        log.info("Index rebuilt via API: region={}, field={}, time={}ms", region, field, elapsed);
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Rebuild all indexes for a region.
     */
    @PostMapping("/api/admin/indexes/{region}/rebuild")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> rebuildRegionIndexes(@PathVariable String region) {
        Map<String, Object> response = new LinkedHashMap<>();
        
        long startTime = System.currentTimeMillis();
        indexManager.rebuildRegionIndexes(region);
        long elapsed = System.currentTimeMillis() - startTime;
        
        response.put("success", true);
        response.put("message", "Region indexes rebuilt successfully");
        response.put("region", region);
        response.put("rebuildTimeMs", elapsed);
        
        log.info("All indexes rebuilt for region via API: region={}, time={}ms", region, elapsed);
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Rebuild all indexes in the system.
     */
    @PostMapping("/api/admin/indexes/rebuild-all")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> rebuildAllIndexes() {
        Map<String, Object> response = new LinkedHashMap<>();
        
        long startTime = System.currentTimeMillis();
        indexManager.rebuildAllIndexes();
        long elapsed = System.currentTimeMillis() - startTime;
        
        response.put("success", true);
        response.put("message", "All indexes rebuilt successfully");
        response.put("rebuildTimeMs", elapsed);
        response.put("statistics", indexManager.getStatistics());
        
        log.info("All indexes rebuilt via API: time={}ms", elapsed);
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get index statistics.
     */
    @GetMapping("/api/admin/indexes/stats")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getIndexStatistics() {
        return ResponseEntity.ok(indexManager.getStatistics());
    }
    
    /**
     * Get statistics for a specific index.
     */
    @GetMapping("/api/admin/indexes/{region}/{field}/stats")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getIndexStats(
            @PathVariable String region,
            @PathVariable String field) {
        
        SecondaryIndex index = indexManager.getIndex(region, field);
        
        if (index == null) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("success", false);
            response.put("error", "Index not found");
            return ResponseEntity.notFound().build();
        }
        
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("region", region);
        response.putAll(index.getStatistics());
        return ResponseEntity.ok(response);
    }
    
    /**
     * Drop all indexes for a region.
     */
    @DeleteMapping("/api/admin/indexes/{region}")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> dropAllRegionIndexes(@PathVariable String region) {
        Map<String, Object> response = new LinkedHashMap<>();
        
        indexManager.dropAllIndexes(region);
        
        response.put("success", true);
        response.put("message", "All indexes dropped for region");
        response.put("region", region);
        
        log.info("All indexes dropped for region via API: region={}", region);
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Reload index configuration from file.
     */
    @PostMapping("/api/admin/indexes/reload-config")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> reloadConfiguration() {
        Map<String, Object> response = new LinkedHashMap<>();
        
        indexConfiguration.loadConfiguration();
        
        response.put("success", true);
        response.put("message", "Configuration reloaded successfully");
        response.put("regions", indexConfiguration.getRegionConfigs().size());
        
        return ResponseEntity.ok(response);
    }
    
    // ==================== Request DTOs ====================
    
    @Data
    public static class CreateIndexRequest {
        private String field;
        private String type = "hash";
        private String description;
        private boolean rebuildNow = true;
    }
}
