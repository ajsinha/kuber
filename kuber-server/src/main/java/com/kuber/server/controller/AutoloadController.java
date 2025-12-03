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

import com.kuber.server.autoload.AutoloadService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for autoload operations.
 */
@RestController
@RequestMapping("/api/autoload")
@RequiredArgsConstructor
public class AutoloadController {
    
    private final AutoloadService autoloadService;
    
    /**
     * Get autoload statistics.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(autoloadService.getStatistics());
    }
    
    /**
     * Trigger an immediate scan.
     */
    @PostMapping("/trigger")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<Map<String, Object>> triggerScan() {
        autoloadService.triggerScan();
        Map<String, Object> response = Map.of(
                "message", "Scan triggered successfully",
                "statistics", autoloadService.getStatistics()
        );
        return ResponseEntity.ok(response);
    }
}
