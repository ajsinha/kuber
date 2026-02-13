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

import com.kuber.server.backup.BackupRestoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API controller for backup and restore operations.
 * 
 * @version 2.4.0
 */
@RestController
@RequestMapping("/api/admin/backup")
@Slf4j
public class BackupController {
    
    private final BackupRestoreService backupRestoreService;
    
    public BackupController(BackupRestoreService backupRestoreService) {
        this.backupRestoreService = backupRestoreService;
    }
    
    /**
     * Get backup/restore statistics.
     * 
     * GET /api/admin/backup/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(backupRestoreService.getStatistics());
    }
    
    /**
     * List available backup files.
     * 
     * GET /api/admin/backup/list
     */
    @GetMapping("/list")
    public ResponseEntity<List<Map<String, Object>>> listBackups() {
        return ResponseEntity.ok(backupRestoreService.listBackups());
    }
    
    /**
     * Trigger immediate backup of all regions.
     * 
     * POST /api/admin/backup
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> backupAll() {
        log.info("Manual backup of all regions triggered via REST API");
        
        Map<String, String> results = backupRestoreService.backupAllRegions();
        
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "completed");
        response.put("regions", results);
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Trigger immediate backup of a single region.
     * 
     * POST /api/admin/backup/{region}
     */
    @PostMapping("/{region}")
    public ResponseEntity<Map<String, Object>> backupRegion(@PathVariable String region) {
        log.info("Manual backup of region '{}' triggered via REST API", region);
        
        Map<String, Object> response = new LinkedHashMap<>();
        
        try {
            Path backupFile = backupRestoreService.backupRegion(region);
            response.put("status", "success");
            response.put("region", region);
            response.put("file", backupFile.toString());
        } catch (Exception e) {
            log.error("Backup failed for region '{}': {}", region, e.getMessage(), e);
            response.put("status", "error");
            response.put("region", region);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Check if a region is currently being restored.
     * 
     * GET /api/admin/backup/restore-status/{region}
     */
    @GetMapping("/restore-status/{region}")
    public ResponseEntity<Map<String, Object>> getRestoreStatus(@PathVariable String region) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("region", region);
        response.put("beingRestored", backupRestoreService.isRegionBeingRestored(region));
        return ResponseEntity.ok(response);
    }
}
