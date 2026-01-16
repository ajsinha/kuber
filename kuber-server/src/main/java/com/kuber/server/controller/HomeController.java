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

import com.kuber.core.model.CacheRegion;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.network.RedisProtocolServer;
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.security.AuthorizationService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Controller for the main dashboard and home pages.
 * Shows only regions the user has access to.
 * 
 * @version 1.8.1
 */
@Controller
@RequiredArgsConstructor
public class HomeController {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final RedisProtocolServer redisServer;
    private final PersistenceStore persistenceStore;
    private final AuthorizationService authorizationService;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    @GetMapping("/")
    public String home(Model model) {
        // Current page for nav highlighting
        model.addAttribute("currentPage", "dashboard");
        
        // Server info
        Map<String, Object> serverInfo = cacheService.getServerInfo();
        model.addAttribute("serverInfo", serverInfo);
        model.addAttribute("nodeId", properties.getNodeId());
        
        // Replication status
        boolean isPrimary = replicationManager == null || replicationManager.isPrimary();
        String mode = replicationManager != null ? replicationManager.getMode() : "STANDALONE";
        model.addAttribute("isPrimary", isPrimary);
        model.addAttribute("replicationMode", mode);
        
        // Authorization info
        boolean isAdmin = authorizationService.isAdmin();
        model.addAttribute("isAdmin", isAdmin);
        
        // Regions - filter to only accessible ones for non-admins
        Collection<CacheRegion> allRegions = cacheService.getAllRegions();
        Collection<CacheRegion> accessibleRegions;
        
        if (isAdmin) {
            accessibleRegions = allRegions;
        } else {
            accessibleRegions = allRegions.stream()
                    .filter(region -> authorizationService.canRead(region.getName()) ||
                                      authorizationService.canWrite(region.getName()) ||
                                      authorizationService.canDelete(region.getName()))
                    .collect(Collectors.toList());
        }
        
        model.addAttribute("regions", accessibleRegions);
        model.addAttribute("regionCount", accessibleRegions.size());
        model.addAttribute("totalRegionCount", allRegions.size());
        
        // Connection stats
        model.addAttribute("activeConnections", redisServer.getActiveConnections());
        model.addAttribute("serverRunning", redisServer.isRunning());
        model.addAttribute("redisPort", properties.getNetwork().getPort());
        
        // Cache stats - for non-admins, only count entries in accessible regions
        long totalEntries;
        if (isAdmin) {
            totalEntries = cacheService.dbSize(null);
        } else {
            totalEntries = accessibleRegions.stream()
                    .mapToLong(r -> cacheService.dbSize(r.getName()))
                    .sum();
        }
        model.addAttribute("totalEntries", totalEntries);
        model.addAttribute("maxMemoryEntries", properties.getCache().getMaxMemoryEntries());
        model.addAttribute("persistentMode", properties.getCache().isPersistentMode());
        
        // Persistence backend info
        model.addAttribute("persistenceType", persistenceStore.getType().name());
        model.addAttribute("persistenceAvailable", persistenceStore.isAvailable());
        model.addAttribute("persistenceConfig", getPersistenceConfigSummary());
        
        return "index";
    }
    
    private String getPersistenceConfigSummary() {
        return switch (persistenceStore.getType()) {
            case MONGODB -> properties.getMongo().getUri();
            case SQLITE -> properties.getPersistence().getSqlite().getPath();
            case POSTGRESQL -> properties.getPersistence().getPostgresql().getUrl();
            case ROCKSDB -> properties.getPersistence().getRocksdb().getPath();
            case LMDB -> properties.getPersistence().getLmdb().getPath();
            case MEMORY -> "In-Memory (no persistence)";
        };
    }
    
    @GetMapping("/login")
    public String login(Model model) {
        // Model attributes like appName are injected by GlobalControllerAdvice
        return "login";
    }
    
    @GetMapping("/about")
    public String about(Model model) {
        model.addAttribute("currentPage", "about");
        
        // Replication status for navbar
        boolean isPrimary = replicationManager == null || replicationManager.isPrimary();
        model.addAttribute("isPrimary", isPrimary);
        
        // Authorization info
        model.addAttribute("isAdmin", authorizationService.isAdmin());
        
        return "about";
    }
}
