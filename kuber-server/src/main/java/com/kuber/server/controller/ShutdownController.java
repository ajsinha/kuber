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

import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.PersistenceOperationLock;
import com.kuber.server.startup.ShutdownFileWatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST API controller for shutdown operations.
 * 
 * <p>Provides endpoints for:
 * <ul>
 *   <li>Triggering graceful shutdown</li>
 *   <li>Checking shutdown status</li>
 *   <li>Viewing shutdown configuration</li>
 * </ul>
 * 
 * <p>All endpoints require API key authentication via X-API-Key header.
 * 
 * @version 2.5.0
 */
@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
@Slf4j
public class ShutdownController {
    
    private final ShutdownFileWatcher shutdownFileWatcher;
    private final PersistenceOperationLock operationLock;
    private final KuberProperties properties;
    
    /**
     * Trigger graceful shutdown of the Kuber server.
     * 
     * <p>This endpoint initiates an orderly shutdown:
     * <ol>
     *   <li>Signals shutdown to block new operations</li>
     *   <li>Stops services in reverse startup order</li>
     *   <li>Persists all cache data</li>
     *   <li>Closes database connections</li>
     * </ol>
     * 
     * @return Confirmation message before shutdown begins
     */
    @PostMapping("/shutdown")
    public ResponseEntity<Map<String, Object>> triggerShutdown(
            @RequestParam(required = false, defaultValue = "API request") String reason) {
        
        if (!properties.getShutdown().isApiEnabled()) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("success", false);
            response.put("message", "Shutdown API is disabled");
            response.put("hint", "Enable with kuber.shutdown.api-enabled=true");
            return ResponseEntity.status(403).body(response);
        }
        
        if (shutdownFileWatcher.isShutdownTriggered()) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("success", false);
            response.put("message", "Shutdown already in progress");
            return ResponseEntity.ok(response);
        }
        
        log.info("Shutdown requested via REST API - reason: {}", reason);
        
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("message", "Graceful shutdown initiated");
        response.put("reason", reason);
        response.put("timestamp", Instant.now().toString());
        response.put("note", "Server will shutdown after all phases complete (~30 seconds)");
        
        // Initiate shutdown in background (after response is sent)
        shutdownFileWatcher.initiateShutdown("REST API: " + reason);
        
        return ResponseEntity.accepted().body(response);
    }
    
    /**
     * Get current shutdown status.
     */
    @GetMapping("/shutdown/status")
    public ResponseEntity<Map<String, Object>> getShutdownStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        
        status.put("shutdownTriggered", shutdownFileWatcher.isShutdownTriggered());
        status.put("shuttingDown", operationLock.isShuttingDown());
        status.put("fileWatcherEnabled", shutdownFileWatcher.isEnabled());
        status.put("shutdownFilePath", shutdownFileWatcher.getShutdownFilePath().toString());
        status.put("apiEnabled", properties.getShutdown().isApiEnabled());
        status.put("phaseDelaySeconds", properties.getShutdown().getPhaseDelaySeconds());
        
        // Include operation lock status
        status.put("operationLockStatus", operationLock.getStatus());
        
        return ResponseEntity.ok(status);
    }
    
    /**
     * Get shutdown configuration.
     */
    @GetMapping("/shutdown/config")
    public ResponseEntity<Map<String, Object>> getShutdownConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        
        KuberProperties.Shutdown shutdownConfig = properties.getShutdown();
        
        config.put("fileEnabled", shutdownConfig.isFileEnabled());
        config.put("filePath", shutdownConfig.getFilePath());
        config.put("checkIntervalMs", shutdownConfig.getCheckIntervalMs());
        config.put("apiEnabled", shutdownConfig.isApiEnabled());
        config.put("phaseDelaySeconds", shutdownConfig.getPhaseDelaySeconds());
        
        // Add usage instructions
        Map<String, String> usage = new LinkedHashMap<>();
        usage.put("file", "touch " + shutdownConfig.getFilePath());
        usage.put("api", "curl -X POST http://localhost:8080/api/admin/shutdown -H 'X-API-Key: your-key'");
        usage.put("script", "./kuber-shutdown.sh");
        config.put("usage", usage);
        
        return ResponseEntity.ok(config);
    }
    
    /**
     * Enable or disable file-based shutdown.
     */
    @PostMapping("/shutdown/file-watcher")
    public ResponseEntity<Map<String, Object>> setFileWatcherEnabled(
            @RequestParam boolean enabled) {
        
        shutdownFileWatcher.setEnabled(enabled);
        
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("fileWatcherEnabled", enabled);
        response.put("message", "Shutdown file watcher " + (enabled ? "enabled" : "disabled"));
        
        return ResponseEntity.ok(response);
    }
}
