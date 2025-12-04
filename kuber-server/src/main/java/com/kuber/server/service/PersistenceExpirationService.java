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
package com.kuber.server.service;

import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.PersistenceStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service that periodically cleans up expired entries from the persistence store.
 * This ensures that TTL-expired entries are removed from durable storage,
 * not just from the in-memory cache.
 */
@Slf4j
@Service
public class PersistenceExpirationService {
    
    private final PersistenceStore persistenceStore;
    private final KuberProperties properties;
    
    // Statistics
    private final AtomicLong totalExpiredDeleted = new AtomicLong(0);
    private final AtomicLong lastRunDeleted = new AtomicLong(0);
    private volatile Instant lastRunTime;
    private volatile Duration lastRunDuration;
    private volatile boolean enabled = true;
    
    public PersistenceExpirationService(PersistenceStore persistenceStore, 
                                        KuberProperties properties) {
        this.persistenceStore = persistenceStore;
        this.properties = properties;
    }
    
    @PostConstruct
    public void init() {
        log.info("Persistence Expiration Service initialized");
        log.info("  - Cleanup interval: {} seconds", getCleanupIntervalSeconds());
        log.info("  - Persistence type: {}", persistenceStore.getType());
    }
    
    /**
     * Scheduled task to clean up expired entries from persistence.
     * Runs every 60 seconds by default (configurable).
     */
    @Scheduled(fixedDelayString = "${kuber.expiration.cleanup-interval-ms:60000}")
    public void cleanupExpiredEntries() {
        if (!enabled) {
            return;
        }
        
        if (!persistenceStore.isAvailable()) {
            log.debug("Persistence store not available, skipping expiration cleanup");
            return;
        }
        
        Instant startTime = Instant.now();
        
        try {
            long deleted = persistenceStore.deleteAllExpiredEntries();
            
            lastRunTime = startTime;
            lastRunDuration = Duration.between(startTime, Instant.now());
            lastRunDeleted.set(deleted);
            totalExpiredDeleted.addAndGet(deleted);
            
            if (deleted > 0) {
                log.info("Persistence expiration cleanup: deleted {} expired entries in {}ms",
                        deleted, lastRunDuration.toMillis());
            } else {
                log.debug("Persistence expiration cleanup: no expired entries found");
            }
            
        } catch (Exception e) {
            log.error("Error during persistence expiration cleanup: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Manually trigger expiration cleanup.
     */
    public long triggerCleanup() {
        log.info("Manual expiration cleanup triggered");
        Instant startTime = Instant.now();
        
        long deleted = persistenceStore.deleteAllExpiredEntries();
        
        lastRunTime = startTime;
        lastRunDuration = Duration.between(startTime, Instant.now());
        lastRunDeleted.set(deleted);
        totalExpiredDeleted.addAndGet(deleted);
        
        log.info("Manual expiration cleanup: deleted {} expired entries in {}ms",
                deleted, lastRunDuration.toMillis());
        
        return deleted;
    }
    
    /**
     * Clean up expired entries for a specific region.
     */
    public long cleanupRegion(String region) {
        log.info("Expiration cleanup triggered for region '{}'", region);
        long deleted = persistenceStore.deleteExpiredEntries(region);
        totalExpiredDeleted.addAndGet(deleted);
        return deleted;
    }
    
    // ==================== Status & Statistics ====================
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        log.info("Persistence expiration service {}", enabled ? "enabled" : "disabled");
    }
    
    public long getTotalExpiredDeleted() {
        return totalExpiredDeleted.get();
    }
    
    public long getLastRunDeleted() {
        return lastRunDeleted.get();
    }
    
    public Instant getLastRunTime() {
        return lastRunTime;
    }
    
    public Duration getLastRunDuration() {
        return lastRunDuration;
    }
    
    public long getCleanupIntervalSeconds() {
        // Default 60 seconds
        return 60;
    }
}
