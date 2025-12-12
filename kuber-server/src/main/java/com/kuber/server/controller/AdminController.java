/*
 * Copyright (c) 2025-2030, All Rights Reserved
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

import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.LmdbPersistenceStore;
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.persistence.PersistenceStore.PersistenceType;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.security.ApiKey;
import com.kuber.server.security.ApiKeyService;
import com.kuber.server.security.JsonUserDetailsService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.*;

/**
 * Controller for administrative operations.
 * Manages users (read-only from users.json) and API keys.
 *
 * @version 1.5.0
 */
@Controller
@RequestMapping("/admin")
@PreAuthorize("hasRole('ADMIN')")
@RequiredArgsConstructor
public class AdminController {
    
    private final JsonUserDetailsService userService;
    private final ApiKeyService apiKeyService;
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final PersistenceStore persistenceStore;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    @ModelAttribute
    public void addCurrentPage(Model model) {
        model.addAttribute("currentPage", "admin");
    }
    
    @GetMapping
    public String adminDashboard(Model model) {
        model.addAttribute("serverInfo", cacheService.getServerInfo());
        model.addAttribute("properties", properties);
        
        // Add persistence info
        model.addAttribute("persistenceInfo", getPersistenceInfo());
        model.addAttribute("persistenceType", persistenceStore.getType().name());
        
        // Add API key stats
        model.addAttribute("apiKeyStats", apiKeyService.getStatistics());
        
        if (replicationManager != null) {
            model.addAttribute("replicationInfo", replicationManager.getReplicationInfo());
        }
        
        return "admin/dashboard";
    }
    
    private Map<String, Object> getPersistenceInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("Type", persistenceStore.getType().name());
        info.put("Available", persistenceStore.isAvailable() ? "Yes" : "No");
        
        String configuredType = properties.getPersistence().getType();
        info.put("Configured", configuredType);
        
        // Add backend-specific details
        switch (persistenceStore.getType()) {
            case MONGODB -> {
                info.put("URI", properties.getMongo().getUri());
                info.put("Database", properties.getMongo().getDatabase());
                info.put("Pool Size", properties.getMongo().getConnectionPoolSize());
            }
            case SQLITE -> {
                info.put("Path", properties.getPersistence().getSqlite().getPath());
            }
            case POSTGRESQL -> {
                info.put("URL", properties.getPersistence().getPostgresql().getUrl());
                info.put("Username", properties.getPersistence().getPostgresql().getUsername());
                info.put("Pool Size", properties.getPersistence().getPostgresql().getPoolSize());
            }
            case ROCKSDB -> {
                info.put("Path", properties.getPersistence().getRocksdb().getPath());
            }
            case LMDB -> {
                info.put("Path", properties.getPersistence().getLmdb().getPath());
                info.put("Map Size", formatBytes(properties.getPersistence().getLmdb().getMapSize()));
            }
            case MEMORY -> {
                info.put("Note", "Data is not persisted across restarts");
            }
        }
        
        return info;
    }
    
    // ==================== User Management ====================
    
    @GetMapping("/users")
    public String listUsers(Model model) {
        Collection<JsonUserDetailsService.JsonUser> users = userService.getAllUsers();
        model.addAttribute("users", users);
        model.addAttribute("usersFile", properties.getSecurity().getUsersFile());
        return "admin/users";
    }
    
    @GetMapping("/users/{userId}")
    public String viewUser(@PathVariable String userId, Model model) {
        JsonUserDetailsService.JsonUser user = userService.getUser(userId);
        
        if (user == null) {
            return "redirect:/admin/users";
        }
        
        model.addAttribute("user", user);
        return "admin/user-detail";
    }
    
    @PostMapping("/users/reload")
    public String reloadUsers(RedirectAttributes redirectAttributes) {
        try {
            userService.reloadUsers();
            redirectAttributes.addFlashAttribute("success", "Users reloaded from file successfully");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to reload users: " + e.getMessage());
        }
        return "redirect:/admin/users";
    }
    
    // ==================== API Key Management ====================
    
    @GetMapping("/apikeys")
    public String listApiKeys(Model model) {
        List<ApiKey> keys = apiKeyService.getAllKeys();
        model.addAttribute("apiKeys", keys);
        model.addAttribute("apiKeyStats", apiKeyService.getStatistics());
        model.addAttribute("users", userService.getAllUsers());
        return "admin/apikeys";
    }
    
    @PostMapping("/apikeys/generate")
    public String generateApiKey(
            @RequestParam String name,
            @RequestParam String userId,
            @RequestParam(required = false) List<String> roles,
            @RequestParam(required = false) Integer expirationDays,
            RedirectAttributes redirectAttributes) {
        
        try {
            if (name == null || name.trim().isEmpty()) {
                redirectAttributes.addFlashAttribute("error", "API key name is required");
                return "redirect:/admin/apikeys";
            }
            
            if (userId == null || userId.trim().isEmpty()) {
                redirectAttributes.addFlashAttribute("error", "User ID is required");
                return "redirect:/admin/apikeys";
            }
            
            // Use default roles if none provided
            if (roles == null || roles.isEmpty()) {
                roles = List.of("USER");
            }
            
            ApiKey apiKey = apiKeyService.generateApiKey(name.trim(), userId.trim(), roles, expirationDays);
            
            // Flash the full key value so user can copy it (only shown once)
            redirectAttributes.addFlashAttribute("success", "API key generated successfully");
            redirectAttributes.addFlashAttribute("newKeyValue", apiKey.getKeyValue());
            redirectAttributes.addFlashAttribute("newKeyName", apiKey.getName());
            
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to generate API key: " + e.getMessage());
        }
        
        return "redirect:/admin/apikeys";
    }
    
    @PostMapping("/apikeys/{keyId}/revoke")
    public String revokeApiKey(@PathVariable String keyId, RedirectAttributes redirectAttributes) {
        try {
            if (apiKeyService.revokeKey(keyId)) {
                redirectAttributes.addFlashAttribute("success", "API key revoked successfully");
            } else {
                redirectAttributes.addFlashAttribute("error", "API key not found");
            }
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to revoke API key: " + e.getMessage());
        }
        return "redirect:/admin/apikeys";
    }
    
    @PostMapping("/apikeys/{keyId}/activate")
    public String activateApiKey(@PathVariable String keyId, RedirectAttributes redirectAttributes) {
        try {
            if (apiKeyService.activateKey(keyId)) {
                redirectAttributes.addFlashAttribute("success", "API key activated successfully");
            } else {
                redirectAttributes.addFlashAttribute("error", "API key not found");
            }
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to activate API key: " + e.getMessage());
        }
        return "redirect:/admin/apikeys";
    }
    
    @PostMapping("/apikeys/{keyId}/delete")
    public String deleteApiKey(@PathVariable String keyId, RedirectAttributes redirectAttributes) {
        try {
            if (apiKeyService.deleteKey(keyId)) {
                redirectAttributes.addFlashAttribute("success", "API key deleted permanently");
            } else {
                redirectAttributes.addFlashAttribute("error", "API key not found");
            }
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to delete API key: " + e.getMessage());
        }
        return "redirect:/admin/apikeys";
    }
    
    @PostMapping("/apikeys/reload")
    public String reloadApiKeys(RedirectAttributes redirectAttributes) {
        try {
            apiKeyService.reloadKeys();
            redirectAttributes.addFlashAttribute("success", "API keys reloaded from file successfully");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to reload API keys: " + e.getMessage());
        }
        return "redirect:/admin/apikeys";
    }
    
    // ==================== System Configuration ====================
    
    @GetMapping("/config")
    public String viewConfig(Model model) {
        model.addAttribute("properties", properties);
        return "admin/config";
    }
    
    // ==================== Statistics ====================
    
    @GetMapping("/stats")
    public String viewStats(Model model) {
        model.addAttribute("serverInfo", cacheService.getServerInfo());
        
        // Get stats for all regions
        cacheService.getAllRegions().forEach(region -> {
            Map<String, Object> stats = cacheService.getStatistics(region.getName());
            model.addAttribute("stats_" + region.getName().replace("-", "_"), stats);
        });
        
        if (replicationManager != null) {
            model.addAttribute("replicationInfo", replicationManager.getReplicationInfo());
        }
        
        return "admin/stats";
    }
    
    // ==================== LMDB Management ====================
    
    /**
     * View LMDB disk usage for all regions.
     */
    @GetMapping("/lmdb/disk-usage")
    @ResponseBody
    public Map<String, Object> getLmdbDiskUsage() {
        Map<String, Object> response = new LinkedHashMap<>();
        
        if (persistenceStore.getType() != PersistenceType.LMDB) {
            response.put("error", "LMDB is not the active persistence backend");
            response.put("currentBackend", persistenceStore.getType().name());
            return response;
        }
        
        LmdbPersistenceStore lmdbStore = (LmdbPersistenceStore) persistenceStore;
        return lmdbStore.getDiskUsageSummary();
    }
    
    /**
     * Get detailed LMDB stats for a specific region.
     */
    @GetMapping("/lmdb/stats/{region}")
    @ResponseBody
    public Map<String, Object> getLmdbRegionStats(@PathVariable String region) {
        Map<String, Object> response = new LinkedHashMap<>();
        
        if (persistenceStore.getType() != PersistenceType.LMDB) {
            response.put("error", "LMDB is not the active persistence backend");
            return response;
        }
        
        LmdbPersistenceStore lmdbStore = (LmdbPersistenceStore) persistenceStore;
        return lmdbStore.getRegionStats(region);
    }
    
    /**
     * Compact an LMDB region to reclaim disk space.
     * 
     * LMDB never shrinks database files automatically - when you delete entries,
     * the pages are added to an internal freelist and reused for future writes.
     * 
     * This endpoint creates a compacted copy of the database with only active data,
     * resulting in a smaller file on disk.
     * 
     * WARNING: This operation briefly closes the region's database. Writes will 
     * be blocked during compaction, but reads should continue from the old file.
     */
    @PostMapping("/lmdb/compact/{region}")
    @ResponseBody
    public Map<String, Object> compactLmdbRegion(@PathVariable String region) {
        Map<String, Object> response = new LinkedHashMap<>();
        
        if (persistenceStore.getType() != PersistenceType.LMDB) {
            response.put("error", "LMDB is not the active persistence backend");
            return response;
        }
        
        LmdbPersistenceStore lmdbStore = (LmdbPersistenceStore) persistenceStore;
        return lmdbStore.compactRegion(region);
    }
    
    /**
     * Compact all LMDB regions.
     */
    @PostMapping("/lmdb/compact-all")
    @ResponseBody
    public Map<String, Object> compactAllLmdbRegions() {
        Map<String, Object> response = new LinkedHashMap<>();
        
        if (persistenceStore.getType() != PersistenceType.LMDB) {
            response.put("error", "LMDB is not the active persistence backend");
            return response;
        }
        
        LmdbPersistenceStore lmdbStore = (LmdbPersistenceStore) persistenceStore;
        
        List<Map<String, Object>> results = new ArrayList<>();
        long totalSaved = 0;
        
        for (String region : cacheService.getRegionNames()) {
            Map<String, Object> result = lmdbStore.compactRegion(region);
            results.add(result);
            
            if (Boolean.TRUE.equals(result.get("success")) && result.get("savedBytes") != null) {
                totalSaved += (Long) result.get("savedBytes");
            }
        }
        
        response.put("results", results);
        response.put("totalSavedBytes", totalSaved);
        response.put("totalSavedMB", String.format("%.2f", totalSaved / (1024.0 * 1024.0)));
        
        return response;
    }
    
    /**
     * Format bytes to human readable string.
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
