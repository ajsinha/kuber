/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.persistence;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Centralized lock service for coordinating persistence operations.
 * 
 * <p>This service prevents concurrent execution of operations that affect 
 * the persistence store, such as:
 * <ul>
 *   <li>Compaction - reorganizes database files</li>
 *   <li>Cleanup/Expiration - deletes expired entries</li>
 *   <li>Autoload - loads data from files</li>
 *   <li>Region Loading - loads data during startup</li>
 *   <li>Shutdown - closes database connections</li>
 * </ul>
 * 
 * <p>Operations are coordinated at two levels:
 * <ul>
 *   <li><b>Global operations</b> (compaction, shutdown) - acquire exclusive global lock</li>
 *   <li><b>Region operations</b> (autoload, cleanup) - acquire per-region lock</li>
 * </ul>
 * 
 * <p>Also tracks which regions are currently loading to block queries to those regions.
 * 
 * @version 2.6.3
 */
@Component
@Slf4j
public class PersistenceOperationLock {
    
    /**
     * Type of persistence operation for lock tracking.
     */
    public enum OperationType {
        COMPACTION("Compaction"),
        CLEANUP("Cleanup"),
        AUTOLOAD("Autoload"),
        REGION_LOADING("Region Loading"),
        SHUTDOWN("Shutdown"),
        DATA_RECOVERY("Data Recovery");
        
        private final String displayName;
        
        OperationType(String displayName) {
            this.displayName = displayName;
        }
        
        public String getDisplayName() {
            return displayName;
        }
    }
    
    // Global lock for exclusive operations (compaction, shutdown)
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(true);
    
    // Per-region locks for region-specific operations
    private final Map<String, ReentrantReadWriteLock> regionLocks = new ConcurrentHashMap<>();
    
    // Track which regions are currently loading (queries should wait)
    private final Set<String> regionsLoading = ConcurrentHashMap.newKeySet();
    
    // Shutdown state - once set, all operations should be blocked
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    
    // Track current operation for status reporting
    private volatile OperationType currentOperation = null;
    private volatile String currentOperationDetails = null;
    private volatile Instant operationStartTime = null;
    
    // Default timeout for acquiring locks
    private static final long DEFAULT_TIMEOUT_SECONDS = 300; // 5 minutes
    
    /**
     * Acquire exclusive global lock for operations like compaction or shutdown.
     * This blocks ALL other operations on ALL regions.
     * 
     * @param operation The operation type acquiring the lock
     * @param details Optional details about the operation
     * @return true if lock acquired, false if timeout
     */
    public boolean acquireExclusiveGlobalLock(OperationType operation, String details) {
        return acquireExclusiveGlobalLock(operation, details, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Acquire exclusive global lock with custom timeout.
     */
    public boolean acquireExclusiveGlobalLock(OperationType operation, String details, 
                                               long timeout, TimeUnit unit) {
        if (shuttingDown.get() && operation != OperationType.SHUTDOWN) {
            log.warn("Cannot acquire lock for {} - system is shutting down", operation.getDisplayName());
            return false;
        }
        
        try {
            log.debug("Attempting to acquire exclusive global lock for {}: {}", 
                    operation.getDisplayName(), details);
            
            if (globalLock.writeLock().tryLock(timeout, unit)) {
                currentOperation = operation;
                currentOperationDetails = details;
                operationStartTime = Instant.now();
                
                log.info("Acquired exclusive global lock for {}: {}", 
                        operation.getDisplayName(), details);
                return true;
            } else {
                log.warn("Timeout acquiring exclusive global lock for {}: {} (waited {} {})", 
                        operation.getDisplayName(), details, timeout, unit);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while acquiring exclusive global lock for {}", 
                    operation.getDisplayName());
            return false;
        }
    }
    
    /**
     * Release exclusive global lock.
     */
    public void releaseExclusiveGlobalLock() {
        if (globalLock.writeLock().isHeldByCurrentThread()) {
            log.info("Releasing exclusive global lock for {}", 
                    currentOperation != null ? currentOperation.getDisplayName() : "unknown");
            currentOperation = null;
            currentOperationDetails = null;
            operationStartTime = null;
            globalLock.writeLock().unlock();
        }
    }
    
    /**
     * Acquire shared global lock for non-exclusive operations.
     * Multiple shared locks can be held simultaneously.
     */
    public boolean acquireSharedGlobalLock(OperationType operation, String details) {
        return acquireSharedGlobalLock(operation, details, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Acquire shared global lock with custom timeout.
     */
    public boolean acquireSharedGlobalLock(OperationType operation, String details,
                                            long timeout, TimeUnit unit) {
        if (shuttingDown.get()) {
            log.debug("Cannot acquire shared lock for {} - system is shutting down", 
                    operation.getDisplayName());
            return false;
        }
        
        try {
            if (globalLock.readLock().tryLock(timeout, unit)) {
                log.debug("Acquired shared global lock for {}: {}", 
                        operation.getDisplayName(), details);
                return true;
            } else {
                log.warn("Timeout acquiring shared global lock for {}: {}", 
                        operation.getDisplayName(), details);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Release shared global lock.
     */
    public void releaseSharedGlobalLock() {
        globalLock.readLock().unlock();
    }
    
    /**
     * Acquire exclusive lock for a specific region.
     * This allows other regions to continue operating.
     */
    public boolean acquireRegionLock(String region, OperationType operation, String details) {
        return acquireRegionLock(region, operation, details, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Acquire exclusive region lock with custom timeout.
     */
    public boolean acquireRegionLock(String region, OperationType operation, String details,
                                      long timeout, TimeUnit unit) {
        if (shuttingDown.get()) {
            log.debug("Cannot acquire region lock for {} - system is shutting down", region);
            return false;
        }
        
        // First acquire shared global lock to ensure no exclusive global operation is running
        if (!acquireSharedGlobalLock(operation, details, timeout, unit)) {
            return false;
        }
        
        ReentrantReadWriteLock regionLock = regionLocks.computeIfAbsent(region, 
                k -> new ReentrantReadWriteLock(true));
        
        try {
            if (regionLock.writeLock().tryLock(timeout, unit)) {
                log.debug("Acquired lock for region '{}' - {}: {}", 
                        region, operation.getDisplayName(), details);
                return true;
            } else {
                log.warn("Timeout acquiring lock for region '{}' - {}: {}", 
                        region, operation.getDisplayName(), details);
                releaseSharedGlobalLock();
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            releaseSharedGlobalLock();
            return false;
        }
    }
    
    /**
     * Release exclusive region lock.
     */
    public void releaseRegionLock(String region) {
        ReentrantReadWriteLock regionLock = regionLocks.get(region);
        if (regionLock != null && regionLock.writeLock().isHeldByCurrentThread()) {
            regionLock.writeLock().unlock();
            log.debug("Released lock for region '{}'", region);
        }
        releaseSharedGlobalLock();
    }
    
    /**
     * Mark a region as loading. Queries to this region should wait.
     */
    public void markRegionLoading(String region) {
        regionsLoading.add(region);
        log.debug("Region '{}' marked as loading", region);
    }
    
    /**
     * Mark a region as loaded. Queries can now proceed.
     */
    public void markRegionLoaded(String region) {
        regionsLoading.remove(region);
        log.debug("Region '{}' marked as loaded", region);
    }
    
    /**
     * Check if a region is currently loading.
     */
    public boolean isRegionLoading(String region) {
        return regionsLoading.contains(region);
    }
    
    /**
     * Wait for a region to finish loading.
     * 
     * @param region Region name
     * @param timeout Maximum wait time
     * @param unit Time unit
     * @return true if region is ready, false if timeout
     */
    public boolean waitForRegionReady(String region, long timeout, TimeUnit unit) {
        if (!regionsLoading.contains(region)) {
            return true;
        }
        
        long startTime = System.currentTimeMillis();
        long timeoutMs = unit.toMillis(timeout);
        
        while (regionsLoading.contains(region)) {
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                log.warn("Timeout waiting for region '{}' to finish loading", region);
                return false;
            }
            
            try {
                Thread.sleep(100); // Check every 100ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Signal that shutdown is starting. Blocks all new operations.
     */
    public void signalShutdown() {
        shuttingDown.set(true);
        log.info("Shutdown signal received - blocking new persistence operations");
    }
    
    /**
     * Check if system is shutting down.
     */
    public boolean isShuttingDown() {
        return shuttingDown.get();
    }
    
    /**
     * Get the set of regions currently loading.
     */
    public Set<String> getRegionsLoading() {
        return Set.copyOf(regionsLoading);
    }
    
    /**
     * Check if any exclusive global operation is in progress.
     */
    public boolean isExclusiveOperationInProgress() {
        return globalLock.isWriteLocked();
    }
    
    /**
     * Get status information about current operations.
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        status.put("shuttingDown", shuttingDown.get());
        status.put("exclusiveOperationInProgress", globalLock.isWriteLocked());
        status.put("currentOperation", currentOperation != null ? currentOperation.getDisplayName() : null);
        status.put("currentOperationDetails", currentOperationDetails);
        status.put("operationStartTime", operationStartTime != null ? operationStartTime.toString() : null);
        status.put("regionsLoading", Set.copyOf(regionsLoading));
        status.put("activeRegionLocks", regionLocks.entrySet().stream()
                .filter(e -> e.getValue().isWriteLocked())
                .map(Map.Entry::getKey)
                .toList());
        return status;
    }
}
