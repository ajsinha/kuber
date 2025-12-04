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
import com.kuber.server.persistence.RocksDbPersistenceStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service that performs scheduled compaction of RocksDB to reclaim disk space.
 * Only active when RocksDB persistence is enabled and compaction is configured.
 * 
 * Compaction removes deleted/expired entries from SST files, reclaiming disk space
 * that would otherwise accumulate over time.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "kuber.persistence.type", havingValue = "rocksdb", matchIfMissing = true)
public class RocksDbCompactionService {
    
    private final PersistenceStore persistenceStore;
    private final KuberProperties properties;
    
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean compactionInProgress = new AtomicBoolean(false);
    private final AtomicLong totalCompactions = new AtomicLong(0);
    private final AtomicLong totalReclaimedBytes = new AtomicLong(0);
    
    private Instant lastCompactionTime;
    private Duration lastCompactionDuration;
    private long lastReclaimedBytes;
    private String lastCompactionMessage;
    
    public RocksDbCompactionService(PersistenceStore persistenceStore, KuberProperties properties) {
        this.persistenceStore = persistenceStore;
        this.properties = properties;
    }
    
    @PostConstruct
    public void initialize() {
        if (persistenceStore instanceof RocksDbPersistenceStore) {
            boolean compactionEnabled = properties.getPersistence().getRocksdb().isCompactionEnabled();
            int intervalMinutes = properties.getPersistence().getRocksdb().getCompactionIntervalMinutes();
            
            enabled.set(compactionEnabled);
            
            if (compactionEnabled) {
                log.info("RocksDB compaction service initialized - interval: {} minutes", intervalMinutes);
            } else {
                log.info("RocksDB compaction service is disabled via configuration");
            }
        } else {
            enabled.set(false);
            log.debug("RocksDB compaction service not applicable - persistence type is {}", 
                    persistenceStore.getType());
        }
    }
    
    /**
     * Scheduled compaction task.
     * Runs based on configured interval (default: 30 minutes).
     * The interval is converted from minutes to milliseconds.
     */
    @Scheduled(fixedRateString = "#{${kuber.persistence.rocksdb.compaction-interval-minutes:30} * 60 * 1000}")
    public void scheduledCompaction() {
        if (!enabled.get()) {
            return;
        }
        
        if (!(persistenceStore instanceof RocksDbPersistenceStore)) {
            return;
        }
        
        triggerCompaction();
    }
    
    /**
     * Manually trigger a compaction operation.
     * 
     * @return CompactionStats with results of the operation
     */
    public CompactionStats triggerCompaction() {
        if (!(persistenceStore instanceof RocksDbPersistenceStore rocksDb)) {
            return new CompactionStats(false, "RocksDB persistence not in use", 
                    0, Duration.ZERO, 0, 0, 0);
        }
        
        if (!compactionInProgress.compareAndSet(false, true)) {
            return new CompactionStats(false, "Compaction already in progress", 
                    totalCompactions.get(), Duration.ZERO, 0, 
                    totalReclaimedBytes.get(), 0);
        }
        
        try {
            log.info("Starting scheduled RocksDB compaction...");
            Instant startTime = Instant.now();
            
            RocksDbPersistenceStore.CompactionResult result = rocksDb.compact();
            
            Duration duration = Duration.between(startTime, Instant.now());
            lastCompactionTime = startTime;
            lastCompactionDuration = duration;
            lastReclaimedBytes = result.reclaimedBytes();
            lastCompactionMessage = result.message();
            
            if (result.success()) {
                totalCompactions.incrementAndGet();
                totalReclaimedBytes.addAndGet(result.reclaimedBytes());
                
                log.info("RocksDB compaction completed: {} column families, {}ms, reclaimed {} MB",
                        result.columnFamiliesCompacted(),
                        duration.toMillis(),
                        String.format("%.2f", result.reclaimedMB()));
            } else {
                log.warn("RocksDB compaction failed: {}", result.message());
            }
            
            return new CompactionStats(
                    result.success(),
                    result.message(),
                    totalCompactions.get(),
                    duration,
                    result.reclaimedBytes(),
                    totalReclaimedBytes.get(),
                    result.columnFamiliesCompacted()
            );
            
        } finally {
            compactionInProgress.set(false);
        }
    }
    
    /**
     * Check if compaction is currently in progress.
     */
    public boolean isCompactionInProgress() {
        return compactionInProgress.get();
    }
    
    /**
     * Check if the service is enabled.
     */
    public boolean isEnabled() {
        return enabled.get();
    }
    
    /**
     * Enable or disable the compaction service.
     */
    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
        log.info("RocksDB compaction service {}", enabled ? "enabled" : "disabled");
    }
    
    /**
     * Get the configured compaction interval in minutes.
     */
    public int getCompactionIntervalMinutes() {
        return properties.getPersistence().getRocksdb().getCompactionIntervalMinutes();
    }
    
    /**
     * Get comprehensive statistics about the compaction service.
     */
    public CompactionServiceStats getStats() {
        RocksDbPersistenceStore.RocksDbStats rocksStats = null;
        if (persistenceStore instanceof RocksDbPersistenceStore rocksDb) {
            rocksStats = rocksDb.getStats();
        }
        
        return new CompactionServiceStats(
                enabled.get(),
                compactionInProgress.get(),
                totalCompactions.get(),
                totalReclaimedBytes.get(),
                properties.getPersistence().getRocksdb().getCompactionIntervalMinutes(),
                lastCompactionTime,
                lastCompactionDuration,
                lastReclaimedBytes,
                lastCompactionMessage,
                rocksStats
        );
    }
    
    /**
     * Result of a single compaction operation.
     */
    public record CompactionStats(
            boolean success,
            String message,
            long totalCompactions,
            Duration duration,
            long reclaimedBytes,
            long totalReclaimedBytes,
            int columnFamiliesCompacted
    ) {
        public double reclaimedMB() {
            return reclaimedBytes / (1024.0 * 1024.0);
        }
        
        public double totalReclaimedMB() {
            return totalReclaimedBytes / (1024.0 * 1024.0);
        }
    }
    
    /**
     * Comprehensive compaction service statistics.
     */
    public record CompactionServiceStats(
            boolean enabled,
            boolean compactionInProgress,
            long totalCompactions,
            long totalReclaimedBytes,
            int intervalMinutes,
            Instant lastCompactionTime,
            Duration lastCompactionDuration,
            long lastReclaimedBytes,
            String lastCompactionMessage,
            RocksDbPersistenceStore.RocksDbStats rocksDbStats
    ) {
        public double totalReclaimedMB() {
            return totalReclaimedBytes / (1024.0 * 1024.0);
        }
        
        public double lastReclaimedMB() {
            return lastReclaimedBytes / (1024.0 * 1024.0);
        }
    }
}
