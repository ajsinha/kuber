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
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Controller for cache query and modification operations.
 */
@Controller
@RequestMapping("/cache")
@RequiredArgsConstructor
public class CacheController {
    
    private final CacheService cacheService;
    
    @ModelAttribute
    public void addCurrentPage(Model model) {
        model.addAttribute("currentPage", "cache");
    }
    
    @GetMapping
    public String cachePage(Model model, 
                           @RequestParam(defaultValue = "default") String region) {
        Collection<CacheRegion> regions = cacheService.getAllRegions();
        model.addAttribute("regions", regions);
        model.addAttribute("currentRegion", region);
        
        // Get keys for the region (limit to 100)
        Set<String> keys = cacheService.keys(region, "*");
        model.addAttribute("keys", keys.stream().limit(100).toList());
        model.addAttribute("totalKeys", keys.size());
        
        return "cache";
    }
    
    @GetMapping("/query")
    public String queryPage(Model model) {
        Collection<CacheRegion> regions = cacheService.getAllRegions();
        model.addAttribute("regions", regions);
        return "query";
    }
    
    @PostMapping("/query")
    public String executeQuery(Model model,
                              @RequestParam String region,
                              @RequestParam String queryType,
                              @RequestParam(required = false) String key,
                              @RequestParam(required = false) String jsonQuery) {
        Collection<CacheRegion> regions = cacheService.getAllRegions();
        model.addAttribute("regions", regions);
        model.addAttribute("queryRegion", region);
        model.addAttribute("queryType", queryType);
        model.addAttribute("queryKey", key);
        model.addAttribute("jsonQuery", jsonQuery);
        
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
                List<CacheEntry> results = cacheService.jsonSearch(region, jsonQuery);
                model.addAttribute("results", results);
                model.addAttribute("resultType", "search");
            } else if ("keys".equals(queryType)) {
                Set<String> keys = cacheService.keys(region, key != null ? key : "*");
                model.addAttribute("results", keys);
                model.addAttribute("resultType", "keys");
            } else if ("ksearch".equals(queryType)) {
                // Regex key search returning key-value pairs
                List<Map<String, Object>> results = cacheService.searchKeysByRegex(
                        region, key != null ? key : ".*", 100);
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
    
    @GetMapping("/entry")
    public String viewEntry(Model model,
                           @RequestParam String region,
                           @RequestParam String key) {
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
        Collection<CacheRegion> regions = cacheService.getAllRegions();
        model.addAttribute("regions", regions);
        model.addAttribute("currentRegion", region);
        return "insert";
    }
    
    @PostMapping("/insert")
    public String insertEntry(@ModelAttribute EntryForm form,
                             RedirectAttributes redirectAttributes) {
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
}
