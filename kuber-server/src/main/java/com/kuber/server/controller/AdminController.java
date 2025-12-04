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
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.security.JsonUserDetailsService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Controller for administrative operations.
 * Users are managed via users.json file - this provides read-only view.
 */
@Controller
@RequestMapping("/admin")
@PreAuthorize("hasRole('ADMIN')")
@RequiredArgsConstructor
public class AdminController {
    
    private final JsonUserDetailsService userService;
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
            case MEMORY -> {
                info.put("Note", "Data is not persisted across restarts");
            }
        }
        
        return info;
    }
    
    // User Management (Read-Only - users are in users.json)
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
    
    // System Configuration
    @GetMapping("/config")
    public String viewConfig(Model model) {
        model.addAttribute("properties", properties);
        return "admin/config";
    }
    
    // Statistics
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
}
