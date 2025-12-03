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
import com.kuber.server.replication.ReplicationManager;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Collection;
import java.util.Map;

/**
 * Controller for the main dashboard and home pages.
 */
@Controller
@RequiredArgsConstructor
public class HomeController {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final RedisProtocolServer redisServer;
    
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
        
        // Regions
        Collection<CacheRegion> regions = cacheService.getAllRegions();
        model.addAttribute("regions", regions);
        model.addAttribute("regionCount", regions.size());
        
        // Connection stats
        model.addAttribute("activeConnections", redisServer.getActiveConnections());
        model.addAttribute("serverRunning", redisServer.isRunning());
        model.addAttribute("redisPort", properties.getNetwork().getPort());
        
        // Cache stats
        long totalEntries = cacheService.dbSize(null);
        model.addAttribute("totalEntries", totalEntries);
        model.addAttribute("maxMemoryEntries", properties.getCache().getMaxMemoryEntries());
        model.addAttribute("persistentMode", properties.getCache().isPersistentMode());
        
        return "index";
    }
    
    @GetMapping("/login")
    public String login() {
        return "login";
    }
}
