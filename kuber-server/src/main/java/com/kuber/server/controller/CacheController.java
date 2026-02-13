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
import com.kuber.server.publishing.RegionEventPublishingService;
import com.kuber.server.security.AuthorizationService;
import com.kuber.server.security.KuberPermission;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Controller for cache query and modification operations.
 * Enforces RBAC permissions for all operations.
 * 
 * @version 2.4.0
 */
@Slf4j
@Controller
@RequestMapping("/cache")
@RequiredArgsConstructor
public class CacheController {
    
    private static final int DEFAULT_LIMIT = 10000;
    private static final int MAX_LIMIT = 100000;
    
    private final CacheService cacheService;
    private final AuthorizationService authorizationService;
    private final RegionEventPublishingService publishingService;
    private final KuberProperties properties;
    
    @ModelAttribute
    public void addCurrentPage(Model model) {
        model.addAttribute("currentPage", "cache");
    }
    
    /**
     * Get regions the current user can access (has any permission for).
     */
    private Collection<CacheRegion> getAccessibleRegions() {
        return cacheService.getAllRegions().stream()
                .filter(region -> authorizationService.canRead(region.getName()) ||
                                  authorizationService.canWrite(region.getName()) ||
                                  authorizationService.canDelete(region.getName()))
                .collect(Collectors.toList());
    }
    
    /**
     * Get user's permissions for a region to pass to the view.
     */
    private void addPermissionsToModel(Model model, String regionName) {
        model.addAttribute("canRead", authorizationService.canRead(regionName));
        model.addAttribute("canWrite", authorizationService.canWrite(regionName));
        model.addAttribute("canDelete", authorizationService.canDelete(regionName));
        model.addAttribute("isAdmin", authorizationService.isAdmin());
    }
    
    @GetMapping
    public String cachePage(Model model, 
                           @RequestParam(defaultValue = "default") String region,
                           @RequestParam(required = false) Integer limit) {
        // Get only accessible regions
        Collection<CacheRegion> regions = getAccessibleRegions();
        model.addAttribute("regions", regions);
        model.addAttribute("currentRegion", region);
        
        // Add permissions for current region
        addPermissionsToModel(model, region);
        
        // Check read permission
        if (!authorizationService.canRead(region)) {
            model.addAttribute("error", "You do not have READ permission for region '" + region + "'");
            model.addAttribute("keys", Set.of());
            model.addAttribute("totalKeys", 0L);
            model.addAttribute("hasMore", false);
            model.addAttribute("resultCount", 0);
            return "cache";
        }
        
        // Determine effective limit
        int effectiveLimit = getEffectiveLimit(limit);
        model.addAttribute("limit", effectiveLimit);
        
        // Get total count first
        long totalKeys = cacheService.dbSize(region);
        model.addAttribute("totalKeys", totalKeys);
        
        // Get keys with limit + 1 to detect overflow
        Set<String> keys = cacheService.keys(region, "*", effectiveLimit + 1);
        
        // Check if there are more results than limit
        boolean hasMore = keys.size() > effectiveLimit;
        model.addAttribute("hasMore", hasMore);
        model.addAttribute("resultCount", Math.min(keys.size(), effectiveLimit));
        
        // Trim to limit if needed
        if (hasMore) {
            keys = keys.stream().limit(effectiveLimit).collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
        }
        model.addAttribute("keys", keys);
        
        return "cache";
    }
    
    @GetMapping("/query")
    public String queryPage(Model model) {
        Collection<CacheRegion> regions = getAccessibleRegions();
        model.addAttribute("regions", regions);
        model.addAttribute("defaultLimit", DEFAULT_LIMIT);
        model.addAttribute("isAdmin", authorizationService.isAdmin());
        return "query";
    }
    
    @PostMapping("/query")
    public String executeQuery(Model model,
                              @RequestParam String region,
                              @RequestParam String queryType,
                              @RequestParam(required = false) String key,
                              @RequestParam(required = false) String jsonQuery,
                              @RequestParam(required = false) Integer limit) {
        Collection<CacheRegion> regions = getAccessibleRegions();
        model.addAttribute("regions", regions);
        model.addAttribute("queryRegion", region);
        model.addAttribute("queryType", queryType);
        model.addAttribute("queryKey", key);
        model.addAttribute("jsonQuery", jsonQuery);
        model.addAttribute("defaultLimit", DEFAULT_LIMIT);
        
        // Add permissions
        addPermissionsToModel(model, region);
        
        // Check read permission
        if (!authorizationService.canRead(region)) {
            model.addAttribute("error", "You do not have READ permission for region '" + region + "'");
            return "query";
        }
        
        // Determine effective limit
        int effectiveLimit = getEffectiveLimit(limit);
        model.addAttribute("limit", effectiveLimit);
        model.addAttribute("userLimit", limit);
        
        try {
            if ("get".equals(queryType)) {
                String value = cacheService.get(region, key);
                model.addAttribute("result", value);
                model.addAttribute("resultType", "string");
            } else if ("jget".equals(queryType)) {
                JsonNode json = cacheService.jsonGet(region, key);
                model.addAttribute("result", json != null ? JsonUtils.toPrettyJson(json) : null);
                model.addAttribute("resultType", "json");
            } else if ("jsearch".equals(queryType)) {
                // Request limit + 1 to detect overflow
                List<CacheEntry> results = cacheService.jsonSearch(region, jsonQuery, effectiveLimit + 1);
                boolean hasMore = results.size() > effectiveLimit;
                model.addAttribute("hasMore", hasMore);
                model.addAttribute("resultCount", Math.min(results.size(), effectiveLimit));
                
                if (hasMore) {
                    results = results.subList(0, effectiveLimit);
                }
                model.addAttribute("results", results);
                model.addAttribute("resultType", "search");
            } else if ("keys".equals(queryType)) {
                // Request limit + 1 to detect overflow
                Set<String> keys = cacheService.keys(region, key != null ? key : "*", effectiveLimit + 1);
                boolean hasMore = keys.size() > effectiveLimit;
                model.addAttribute("hasMore", hasMore);
                model.addAttribute("resultCount", Math.min(keys.size(), effectiveLimit));
                
                if (hasMore) {
                    keys = keys.stream().limit(effectiveLimit).collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
                }
                model.addAttribute("results", keys);
                model.addAttribute("resultType", "keys");
            } else if ("ksearch".equals(queryType)) {
                // Request limit + 1 to detect overflow
                List<Map<String, Object>> results = cacheService.searchKeysByRegex(
                        region, key != null ? key : ".*", effectiveLimit + 1);
                boolean hasMore = results.size() > effectiveLimit;
                model.addAttribute("hasMore", hasMore);
                model.addAttribute("resultCount", Math.min(results.size(), effectiveLimit));
                
                if (hasMore) {
                    results = results.subList(0, effectiveLimit);
                }
                model.addAttribute("results", results);
                model.addAttribute("resultType", "ksearch");
            } else if ("hgetall".equals(queryType)) {
                Map<String, String> hash = cacheService.hgetall(region, key);
                model.addAttribute("result", hash);
                model.addAttribute("resultType", "hash");
            }
            model.addAttribute("success", true);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }
        
        return "query";
    }
    
    /**
     * Get effective limit, respecting default and max bounds.
     */
    private int getEffectiveLimit(Integer userLimit) {
        if (userLimit == null || userLimit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(userLimit, MAX_LIMIT);
    }
    
    @GetMapping("/entry")
    public String viewEntry(Model model,
                           @RequestParam String region,
                           @RequestParam String key) {
        // Add permissions
        addPermissionsToModel(model, region);
        
        // Check read permission
        if (!authorizationService.canRead(region)) {
            model.addAttribute("error", "You do not have READ permission for region '" + region + "'");
            model.addAttribute("region", region);
            model.addAttribute("key", key);
            return "entry";
        }
        
        String value = cacheService.get(region, key);
        JsonNode jsonValue = null;
        String type = cacheService.type(region, key);
        long ttl = cacheService.ttl(region, key);
        
        if ("json".equals(type)) {
            jsonValue = cacheService.jsonGet(region, key);
        }
        
        model.addAttribute("region", region);
        model.addAttribute("key", key);
        model.addAttribute("value", value);
        model.addAttribute("jsonValue", jsonValue != null ? JsonUtils.toPrettyJson(jsonValue) : null);
        model.addAttribute("type", type);
        model.addAttribute("ttl", ttl);
        
        return "entry";
    }
    
    @GetMapping("/insert")
    public String insertPage(Model model,
                            @RequestParam(defaultValue = "default") String region) {
        // Get only regions with write permission
        Collection<CacheRegion> regions = cacheService.getAllRegions().stream()
                .filter(r -> authorizationService.canWrite(r.getName()))
                .collect(Collectors.toList());
        model.addAttribute("regions", regions);
        model.addAttribute("currentRegion", region);
        addPermissionsToModel(model, region);
        
        // Check write permission
        if (!authorizationService.canWrite(region)) {
            model.addAttribute("error", "You do not have WRITE permission for region '" + region + "'");
        }
        
        return "insert";
    }
    
    @PostMapping("/insert")
    public String insertEntry(@ModelAttribute EntryForm form,
                             RedirectAttributes redirectAttributes) {
        // Check write permission
        if (!authorizationService.canWrite(form.getRegion())) {
            redirectAttributes.addFlashAttribute("error", 
                    "You do not have WRITE permission for region '" + form.getRegion() + "'");
            return "redirect:/cache?region=" + form.getRegion();
        }
        
        try {
            if ("json".equals(form.getValueType())) {
                JsonNode json = JsonUtils.parse(form.getValue());
                cacheService.jsonSet(form.getRegion(), form.getKey(), "$", json, form.getTtl());
            } else {
                cacheService.set(form.getRegion(), form.getKey(), form.getValue(), form.getTtl());
            }
            
            redirectAttributes.addFlashAttribute("success", 
                    "Entry '" + form.getKey() + "' saved successfully");
            return "redirect:/cache?region=" + form.getRegion();
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", e.getMessage());
            return "redirect:/cache/insert?region=" + form.getRegion();
        }
    }
    
    @PostMapping("/delete")
    public String deleteEntry(@RequestParam String region,
                             @RequestParam String key,
                             RedirectAttributes redirectAttributes) {
        // Check delete permission
        if (!authorizationService.canDelete(region)) {
            redirectAttributes.addFlashAttribute("error", 
                    "You do not have DELETE permission for region '" + region + "'");
            return "redirect:/cache?region=" + region;
        }
        
        try {
            boolean deleted = cacheService.delete(region, key);
            if (deleted) {
                redirectAttributes.addFlashAttribute("success", 
                        "Entry '" + key + "' deleted successfully");
            } else {
                redirectAttributes.addFlashAttribute("warning", "Entry not found");
            }
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", e.getMessage());
        }
        return "redirect:/cache?region=" + region;
    }
    
    @PostMapping("/update")
    public String updateEntry(@ModelAttribute EntryForm form,
                             RedirectAttributes redirectAttributes) {
        // Check write permission
        if (!authorizationService.canWrite(form.getRegion())) {
            redirectAttributes.addFlashAttribute("error", 
                    "You do not have WRITE permission for region '" + form.getRegion() + "'");
            return "redirect:/cache/entry?region=" + form.getRegion() + "&key=" + form.getKey();
        }
        
        try {
            if ("json".equals(form.getValueType())) {
                JsonNode json = JsonUtils.parse(form.getValue());
                cacheService.jsonSet(form.getRegion(), form.getKey(), "$", json, form.getTtl());
            } else {
                cacheService.set(form.getRegion(), form.getKey(), form.getValue(), form.getTtl());
            }
            
            redirectAttributes.addFlashAttribute("success", 
                    "Entry '" + form.getKey() + "' updated successfully");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", e.getMessage());
        }
        return "redirect:/cache/entry?region=" + form.getRegion() + "&key=" + form.getKey();
    }
    
    @Data
    public static class EntryForm {
        private String region;
        private String key;
        private String value;
        private String valueType = "string";
        private long ttl = -1;
    }
    
    /**
     * Publish specified keys from a region as queryresult events to configured brokers.
     * Called via AJAX from the Query page and Cache Browser "Publish As Events" button.
     */
    @PostMapping("/publish-as-events")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> publishAsEvents(@RequestBody PublishEventsRequest request) {
        Map<String, Object> response = new HashMap<>();
        String region = request.getRegion();
        List<String> keys = request.getKeys();
        
        if (region == null || region.isBlank()) {
            response.put("success", false);
            response.put("message", "Region is required");
            return ResponseEntity.badRequest().body(response);
        }
        
        if (keys == null || keys.isEmpty()) {
            response.put("success", false);
            response.put("message", "No keys provided");
            return ResponseEntity.badRequest().body(response);
        }
        
        // Check read permission
        if (!authorizationService.canRead(region)) {
            response.put("success", false);
            response.put("message", "No READ permission for region '" + region + "'");
            return ResponseEntity.status(403).body(response);
        }
        
        // Check if publishing is configured for this region
        if (!publishingService.isPublishingEnabled(region)) {
            response.put("success", false);
            response.put("message", "Event publishing is not configured or not enabled for region '" + region + "'");
            return ResponseEntity.ok(response);
        }
        
        String nodeId = properties.getNodeId();
        int published = 0;
        int skipped = 0;
        
        for (String key : keys) {
            try {
                // Try JSON first, fall back to string
                JsonNode json = cacheService.jsonGet(region, key);
                String value;
                if (json != null) {
                    value = JsonUtils.toJson(json);
                } else {
                    value = cacheService.get(region, key);
                }
                
                if (value != null) {
                    publishingService.publishQueryResult(region, key, value, nodeId);
                    published++;
                } else {
                    skipped++;
                }
            } catch (Exception e) {
                log.warn("Failed to publish query result for key '{}' in region '{}': {}", key, region, e.getMessage());
                skipped++;
            }
        }
        
        response.put("success", true);
        response.put("published", published);
        response.put("skipped", skipped);
        response.put("message", "Published " + published + " event(s) to configured brokers" + 
                (skipped > 0 ? " (" + skipped + " skipped)" : ""));
        
        log.info("Publish As Events: region={}, requested={}, published={}, skipped={}", 
                region, keys.size(), published, skipped);
        
        return ResponseEntity.ok(response);
    }
    
    @Data
    public static class PublishEventsRequest {
        private String region;
        private List<String> keys;
    }
}
