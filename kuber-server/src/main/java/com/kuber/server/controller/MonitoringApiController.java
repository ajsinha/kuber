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

import com.kuber.server.cache.CacheMetricsService;
import com.kuber.server.cache.CacheService;
import com.kuber.server.cache.MemoryWatcherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import com.kuber.server.autoload.AutoloadService;
import com.kuber.server.service.PersistenceExpirationService;
import com.kuber.server.service.RocksDbCompactionService;

/**
 * REST API controller for monitoring and system information.
 */
@Slf4j
@RestController
@RequestMapping("/api/monitoring")
@PreAuthorize("hasRole('ADMIN')")
@RequiredArgsConstructor
public class MonitoringApiController {
    
    private final CacheService cacheService;
    private final CacheMetricsService metricsService;
    
    @Autowired(required = false)
    private MemoryWatcherService memoryWatcherService;
    
    @Autowired(required = false)
    private PersistenceExpirationService expirationService;
    
    @Autowired(required = false)
    private AutoloadService autoloadService;
    
    @Autowired(required = false)
    private RocksDbCompactionService compactionService;
    
    /**
     * Get JVM and system information.
     */
    @GetMapping("/system")
    public ResponseEntity<Map<String, Object>> getSystemInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        
        // Process ID
        info.put("pid", ProcessHandle.current().pid());
        
        try {
            // Host info
            InetAddress localhost = InetAddress.getLocalHost();
            info.put("hostname", localhost.getHostName());
            info.put("ipAddress", localhost.getHostAddress());
        } catch (Exception e) {
            info.put("hostname", "unknown");
            info.put("ipAddress", "unknown");
        }
        
        // OS info
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        info.put("osName", osBean.getName());
        info.put("osVersion", osBean.getVersion());
        info.put("osArch", osBean.getArch());
        info.put("availableProcessors", osBean.getAvailableProcessors());
        info.put("systemLoadAverage", osBean.getSystemLoadAverage());
        
        // JVM info
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        info.put("jvmName", runtimeBean.getVmName());
        info.put("jvmVendor", runtimeBean.getVmVendor());
        info.put("jvmVersion", runtimeBean.getVmVersion());
        info.put("javaVersion", System.getProperty("java.version"));
        
        // Uptime
        long uptimeMs = runtimeBean.getUptime();
        Duration uptime = Duration.ofMillis(uptimeMs);
        info.put("uptimeMs", uptimeMs);
        info.put("uptimeFormatted", formatDuration(uptime));
        info.put("startTime", Instant.ofEpochMilli(runtimeBean.getStartTime()).toString());
        
        return ResponseEntity.ok(info);
    }
    
    /**
     * Get JVM memory information.
     */
    @GetMapping("/memory")
    public ResponseEntity<Map<String, Object>> getMemoryInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
        
        // Heap memory
        Map<String, Object> heap = new LinkedHashMap<>();
        heap.put("used", heapUsage.getUsed());
        heap.put("usedMB", heapUsage.getUsed() / (1024 * 1024));
        heap.put("committed", heapUsage.getCommitted());
        heap.put("committedMB", heapUsage.getCommitted() / (1024 * 1024));
        heap.put("max", heapUsage.getMax());
        heap.put("maxMB", heapUsage.getMax() / (1024 * 1024));
        heap.put("usagePercent", heapUsage.getMax() > 0 
            ? (heapUsage.getUsed() * 100.0) / heapUsage.getMax() : 0);
        info.put("heap", heap);
        
        // Non-heap memory
        Map<String, Object> nonHeap = new LinkedHashMap<>();
        nonHeap.put("used", nonHeapUsage.getUsed());
        nonHeap.put("usedMB", nonHeapUsage.getUsed() / (1024 * 1024));
        nonHeap.put("committed", nonHeapUsage.getCommitted());
        nonHeap.put("committedMB", nonHeapUsage.getCommitted() / (1024 * 1024));
        info.put("nonHeap", nonHeap);
        
        // Runtime memory (simpler view)
        Runtime runtime = Runtime.getRuntime();
        info.put("freeMemory", runtime.freeMemory());
        info.put("freeMemoryMB", runtime.freeMemory() / (1024 * 1024));
        info.put("totalMemory", runtime.totalMemory());
        info.put("totalMemoryMB", runtime.totalMemory() / (1024 * 1024));
        info.put("maxMemory", runtime.maxMemory());
        info.put("maxMemoryMB", runtime.maxMemory() / (1024 * 1024));
        
        return ResponseEntity.ok(info);
    }
    
    /**
     * Trigger garbage collection to free heap memory.
     */
    @PostMapping("/gc")
    public ResponseEntity<Map<String, Object>> triggerGC() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        // Get memory before GC
        Runtime runtime = Runtime.getRuntime();
        long usedBefore = runtime.totalMemory() - runtime.freeMemory();
        
        // Trigger GC
        log.info("Garbage collection triggered by admin");
        System.gc();
        
        // Wait a bit for GC to complete
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Get memory after GC
        long usedAfter = runtime.totalMemory() - runtime.freeMemory();
        long freed = usedBefore - usedAfter;
        
        result.put("success", true);
        result.put("usedBeforeMB", usedBefore / (1024 * 1024));
        result.put("usedAfterMB", usedAfter / (1024 * 1024));
        result.put("freedMB", freed / (1024 * 1024));
        result.put("message", String.format("Freed approximately %d MB", freed / (1024 * 1024)));
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Get cache activity metrics for charts.
     */
    @GetMapping("/metrics/activity")
    public ResponseEntity<Map<String, Object>> getCacheActivity(
            @RequestParam(defaultValue = "30") int minutes) {
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("minutes", minutes);
        result.put("timestamp", Instant.now().toEpochMilli());
        result.put("regions", metricsService.getAllHistoricalMetrics(minutes));
        result.put("globalStats", metricsService.getGlobalStats());
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Get region population data for charts.
     */
    @GetMapping("/metrics/regions")
    public ResponseEntity<List<Map<String, Object>>> getRegionStats() {
        return ResponseEntity.ok(cacheService.getAllRegionStats());
    }
    
    /**
     * Get current minute metrics for a specific region.
     */
    @GetMapping("/metrics/current/{region}")
    public ResponseEntity<Map<String, Long>> getCurrentMetrics(@PathVariable String region) {
        return ResponseEntity.ok(metricsService.getCurrentMetrics(region));
    }
    
    /**
     * Get list of all tracked regions.
     */
    @GetMapping("/metrics/tracked-regions")
    public ResponseEntity<Set<String>> getTrackedRegions() {
        return ResponseEntity.ok(metricsService.getTrackedRegions());
    }
    
    /**
     * Get memory watcher status and statistics.
     */
    @GetMapping("/memory-watcher")
    public ResponseEntity<Map<String, Object>> getMemoryWatcherStats() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (memoryWatcherService == null) {
            result.put("enabled", false);
            result.put("message", "Memory watcher is disabled via configuration");
            return ResponseEntity.ok(result);
        }
        
        result.put("enabled", true);
        
        MemoryWatcherService.MemoryWatcherStats stats = memoryWatcherService.getStats();
        
        // Current state
        result.put("heapUsagePercent", String.format("%.1f", stats.heapUsagePercent()));
        result.put("heapUsedMB", stats.heapUsedMB());
        result.put("heapMaxMB", stats.heapMaxMB());
        result.put("evictionInProgress", stats.evictionInProgress());
        result.put("totalMemoryEntries", cacheService.getTotalMemoryEntries());
        
        // Configuration
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("highWatermarkPercent", stats.highWatermarkPercent());
        config.put("lowWatermarkPercent", stats.lowWatermarkPercent());
        config.put("evictionBatchSize", stats.evictionBatchSize());
        config.put("checkIntervalMs", stats.checkIntervalMs());
        result.put("config", config);
        
        // Totals
        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("evictedEntries", stats.totalEvictedEntries());
        totals.put("evictionCycles", stats.evictionCycles());
        totals.put("checks", stats.totalChecks());
        result.put("totals", totals);
        
        // Last check info
        Map<String, Object> lastCheck = new LinkedHashMap<>();
        lastCheck.put("time", stats.lastCheckTime() != null ? stats.lastCheckTime().toString() : null);
        lastCheck.put("heapPercent", String.format("%.1f", stats.lastCheckHeapPercent()));
        result.put("lastCheck", lastCheck);
        
        // Last eviction info
        Map<String, Object> lastEviction = new LinkedHashMap<>();
        lastEviction.put("time", stats.lastEvictionTime() != null ? stats.lastEvictionTime().toString() : null);
        lastEviction.put("entriesEvicted", stats.lastEvictionCount());
        lastEviction.put("durationMs", stats.lastEvictionDurationMs());
        lastEviction.put("heapBefore", String.format("%.1f", stats.lastEvictionHeapBefore()));
        lastEviction.put("heapAfter", String.format("%.1f", stats.lastEvictionHeapAfter()));
        result.put("lastEviction", lastEviction);
        
        // Last activity message
        result.put("lastActivityMessage", stats.lastActivityMessage());
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Force an eviction cycle.
     */
    @PostMapping("/memory-watcher/evict")
    public ResponseEntity<Map<String, Object>> forceEviction() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (memoryWatcherService == null) {
            result.put("success", false);
            result.put("message", "Memory watcher is disabled");
            return ResponseEntity.ok(result);
        }
        
        if (memoryWatcherService.isEvictionInProgress()) {
            result.put("success", false);
            result.put("message", "Eviction already in progress");
            return ResponseEntity.ok(result);
        }
        
        // Get stats before eviction
        long entriesBefore = cacheService.getTotalMemoryEntries();
        double heapBefore = memoryWatcherService.getHeapUsagePercent();
        
        // Trigger eviction
        log.info("Force eviction triggered by admin");
        memoryWatcherService.forceEviction();
        
        // Get stats after eviction
        long entriesAfter = cacheService.getTotalMemoryEntries();
        double heapAfter = memoryWatcherService.getHeapUsagePercent();
        
        result.put("success", true);
        result.put("entriesBefore", entriesBefore);
        result.put("entriesAfter", entriesAfter);
        result.put("entriesEvicted", entriesBefore - entriesAfter);
        result.put("heapBeforePercent", String.format("%.1f", heapBefore));
        result.put("heapAfterPercent", String.format("%.1f", heapAfter));
        result.put("message", String.format("Evicted %d entries, heap reduced from %.1f%% to %.1f%%",
                entriesBefore - entriesAfter, heapBefore, heapAfter));
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Get autoload service status and statistics.
     */
    @GetMapping("/autoload")
    public ResponseEntity<Map<String, Object>> getAutoloadStats() {
        if (autoloadService == null) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("enabled", false);
            result.put("message", "Autoload service is not available");
            return ResponseEntity.ok(result);
        }
        
        return ResponseEntity.ok(autoloadService.getStatistics());
    }
    
    /**
     * Trigger an immediate autoload scan.
     */
    @PostMapping("/autoload/scan")
    public ResponseEntity<Map<String, Object>> triggerAutoloadScan() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (autoloadService == null) {
            result.put("success", false);
            result.put("message", "Autoload service is not available");
            return ResponseEntity.ok(result);
        }
        
        log.info("Manual autoload scan triggered by admin");
        autoloadService.triggerScan();
        
        result.put("success", true);
        result.put("message", "Scan triggered");
        
        return ResponseEntity.ok(result);
    }
    
    private String formatDuration(Duration duration) {
        long days = duration.toDays();
        long hours = duration.toHours() % 24;
        long minutes = duration.toMinutes() % 60;
        long seconds = duration.getSeconds() % 60;
        
        StringBuilder sb = new StringBuilder();
        if (days > 0) sb.append(days).append("d ");
        if (hours > 0 || days > 0) sb.append(hours).append("h ");
        if (minutes > 0 || hours > 0 || days > 0) sb.append(minutes).append("m ");
        sb.append(seconds).append("s");
        
        return sb.toString();
    }
    
    // ==================== Expiration Service Endpoints ====================
    
    @GetMapping("/expiration")
    public ResponseEntity<Map<String, Object>> getExpirationStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        
        if (expirationService == null) {
            status.put("available", false);
            status.put("message", "Expiration service not available");
            return ResponseEntity.ok(status);
        }
        
        status.put("available", true);
        status.put("enabled", expirationService.isEnabled());
        status.put("totalExpiredDeleted", expirationService.getTotalExpiredDeleted());
        status.put("lastRunDeleted", expirationService.getLastRunDeleted());
        status.put("cleanupIntervalSeconds", expirationService.getCleanupIntervalSeconds());
        
        if (expirationService.getLastRunTime() != null) {
            status.put("lastRunTime", expirationService.getLastRunTime().toString());
            status.put("lastRunDurationMs", expirationService.getLastRunDuration().toMillis());
        }
        
        return ResponseEntity.ok(status);
    }
    
    @PostMapping("/expiration/trigger")
    public ResponseEntity<Map<String, Object>> triggerExpirationCleanup() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (expirationService == null) {
            result.put("success", false);
            result.put("error", "Expiration service not available");
            return ResponseEntity.badRequest().body(result);
        }
        
        long deleted = expirationService.triggerCleanup();
        
        result.put("success", true);
        result.put("deleted", deleted);
        result.put("message", "Cleaned up " + deleted + " expired entries from persistence");
        
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/expiration/trigger/{region}")
    public ResponseEntity<Map<String, Object>> triggerRegionExpirationCleanup(@PathVariable String region) {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (expirationService == null) {
            result.put("success", false);
            result.put("error", "Expiration service not available");
            return ResponseEntity.badRequest().body(result);
        }
        
        long deleted = expirationService.cleanupRegion(region);
        
        result.put("success", true);
        result.put("region", region);
        result.put("deleted", deleted);
        result.put("message", "Cleaned up " + deleted + " expired entries from region '" + region + "'");
        
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/expiration/enable")
    public ResponseEntity<Map<String, Object>> enableExpirationService(@RequestParam boolean enabled) {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (expirationService == null) {
            result.put("success", false);
            result.put("error", "Expiration service not available");
            return ResponseEntity.badRequest().body(result);
        }
        
        expirationService.setEnabled(enabled);
        
        result.put("success", true);
        result.put("enabled", enabled);
        result.put("message", "Expiration service " + (enabled ? "enabled" : "disabled"));
        
        return ResponseEntity.ok(result);
    }
    
    // ==================== RocksDB Compaction Endpoints ====================
    
    /**
     * Get RocksDB compaction service status and statistics.
     */
    @GetMapping("/compaction")
    public ResponseEntity<Map<String, Object>> getCompactionStatus() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (compactionService == null) {
            result.put("available", false);
            result.put("message", "RocksDB compaction service not available (not using RocksDB persistence)");
            return ResponseEntity.ok(result);
        }
        
        RocksDbCompactionService.CompactionServiceStats stats = compactionService.getStats();
        
        result.put("available", true);
        result.put("enabled", stats.enabled());
        result.put("compactionInProgress", stats.compactionInProgress());
        result.put("totalCompactions", stats.totalCompactions());
        result.put("totalReclaimedMB", String.format("%.2f", stats.totalReclaimedMB()));
        result.put("cronExpression", stats.cronExpression());
        
        if (stats.lastCompactionTime() != null) {
            result.put("lastCompactionTime", stats.lastCompactionTime().toString());
            result.put("lastCompactionDurationMs", stats.lastCompactionDuration().toMillis());
            result.put("lastReclaimedMB", String.format("%.2f", stats.lastReclaimedMB()));
            result.put("lastCompactionMessage", stats.lastCompactionMessage());
        }
        
        // Include RocksDB storage stats
        if (stats.rocksDbStats() != null) {
            Map<String, Object> storageStats = new LinkedHashMap<>();
            storageStats.put("path", stats.rocksDbStats().path());
            storageStats.put("sizeMB", String.format("%.2f", stats.rocksDbStats().sizeMB()));
            storageStats.put("regionCount", stats.rocksDbStats().regionCount());
            storageStats.put("totalEntries", stats.rocksDbStats().totalEntries());
            result.put("storage", storageStats);
        }
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Trigger a manual RocksDB compaction.
     */
    @PostMapping("/compaction/trigger")
    public ResponseEntity<Map<String, Object>> triggerCompaction() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (compactionService == null) {
            result.put("success", false);
            result.put("error", "RocksDB compaction service not available");
            return ResponseEntity.badRequest().body(result);
        }
        
        if (compactionService.isCompactionInProgress()) {
            result.put("success", false);
            result.put("error", "Compaction already in progress");
            return ResponseEntity.ok(result);
        }
        
        log.info("Manual RocksDB compaction triggered by admin");
        RocksDbCompactionService.CompactionStats stats = compactionService.triggerCompaction();
        
        result.put("success", stats.success());
        result.put("message", stats.message());
        result.put("durationMs", stats.duration().toMillis());
        result.put("reclaimedMB", String.format("%.2f", stats.reclaimedMB()));
        result.put("regionsCompacted", stats.regionsCompacted());
        result.put("totalCompactions", stats.totalCompactions());
        result.put("totalReclaimedMB", String.format("%.2f", stats.totalReclaimedMB()));
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Enable or disable the RocksDB compaction service.
     */
    @PostMapping("/compaction/enable")
    public ResponseEntity<Map<String, Object>> enableCompactionService(@RequestParam boolean enabled) {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (compactionService == null) {
            result.put("success", false);
            result.put("error", "RocksDB compaction service not available");
            return ResponseEntity.badRequest().body(result);
        }
        
        compactionService.setEnabled(enabled);
        
        result.put("success", true);
        result.put("enabled", enabled);
        result.put("message", "Compaction service " + (enabled ? "enabled" : "disabled"));
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Trigger compaction for a specific region.
     */
    @PostMapping("/compaction/trigger/{region}")
    public ResponseEntity<Map<String, Object>> triggerRegionCompaction(@PathVariable String region) {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (compactionService == null) {
            result.put("success", false);
            result.put("error", "RocksDB compaction service not available");
            return ResponseEntity.badRequest().body(result);
        }
        
        log.info("Manual RocksDB compaction triggered for region '{}' by admin", region);
        var stats = compactionService.triggerRegionCompaction(region);
        
        result.put("success", stats.success());
        result.put("region", region);
        result.put("message", stats.message());
        result.put("durationMs", stats.durationMs());
        result.put("reclaimedMB", String.format("%.2f", stats.reclaimedMB()));
        
        return ResponseEntity.ok(result);
    }
}
