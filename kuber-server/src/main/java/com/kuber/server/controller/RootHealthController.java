/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Root-level health and status endpoints.
 * These mirror the /api/ping, /api/info, /api/status, /api/health endpoints
 * but are accessible at the root path (e.g., http://localhost:8080/ping).
 *
 * This allows health checks and monitoring tools to access these endpoints
 * without knowing the /api or /api/v1 prefix.
 *
 * @version 2.6.0
 */
package com.kuber.server.controller;

import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.search.ParallelJsonSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Root-level health and status endpoints accessible without /api prefix.
 * Mirrors ApiController's /ping, /info, /status, /health endpoints.
 *
 * @version 2.6.0
 */
@RestController
@RequiredArgsConstructor
public class RootHealthController {

    private final CacheService cacheService;
    private final KuberProperties properties;
    private final ParallelJsonSearchService parallelJsonSearchService;

    @Autowired(required = false)
    private ReplicationManager replicationManager;

    /**
     * Root-level ping endpoint.
     * GET /ping
     */
    @GetMapping("/ping")
    public ResponseEntity<Map<String, Object>> ping() {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "OK");
        result.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(result);
    }

    /**
     * Root-level server info endpoint.
     * GET /info
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getInfo() {
        Map<String, Object> info = cacheService.getServerInfo();
        if (replicationManager != null) {
            info.put("replication", replicationManager.getReplicationInfo());
        }
        return ResponseEntity.ok(info);
    }

    /**
     * Root-level status endpoint.
     * GET /status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "UP");
        status.put("nodeId", properties.getNodeId());
        status.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        status.put("version", properties.getVersion());
        return ResponseEntity.ok(status);
    }

    /**
     * Root-level health check endpoint.
     * GET /health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("nodeId", properties.getNodeId());
        health.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        return ResponseEntity.ok(health);
    }

    /**
     * Root-level stats endpoint.
     * GET /stats
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = cacheService.getServerInfo();
        stats.put("searchStats", parallelJsonSearchService.getStatistics());
        return ResponseEntity.ok(stats);
    }
}
